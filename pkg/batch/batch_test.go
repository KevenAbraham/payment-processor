package batch_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KevenAbraham/payment-processor/pkg/batch"
)

func TestSubmitAfterClose(t *testing.T) {
	echo := func(_ context.Context, items []int) ([]int, error) { return items, nil }
	b := batch.New(2, time.Millisecond, echo)
	b.Close()

	_, err := b.Submit(context.Background(), 1)
	if !errors.Is(err, batch.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestFlushByTimeWindow(t *testing.T) {
	echo := func(_ context.Context, items []string) ([]string, error) {
		return items, nil
	}

	b := batch.New(100, 20*time.Millisecond, echo)
	defer b.Close()

	start := time.Now()
	value, err := b.Submit(context.Background(), "lonely")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if value != "lonely" {
		t.Errorf("expected lonely, got %q", value)
	}

	if elapsed > 500*time.Millisecond {
		t.Errorf("expected flush within 500ms, took %v", elapsed)
	}
}

func TestBatchBySize(t *testing.T) {
	var calls atomic.Int32
	double := func(_ context.Context, items []int) ([]int, error) {
		calls.Add(1)
		results := make([]int, len(items))
		for i, item := range items {
			results[i] = item * 2
		}
		return results, nil
	}

	b := batch.New(4, time.Second, double)
	defer b.Close()

	results := make([]int, 8)
	var wg sync.WaitGroup

	for i := range results {
		wg.Go(func() {
			value, err := b.Submit(context.Background(), i)
			if err != nil {
				t.Errorf("unexpected error for item %d: %v", i, err)
				return
			}
			results[i] = value
		})
	}
	wg.Wait()

	for i := range results {
		if results[i] != i*2 {
			t.Errorf("expected result[%d] = %d, got %d", i, i*2, results[i])
		}
	}

	if got := calls.Load(); got > 4 {
		t.Errorf("expected at most 4 flusher calls, got %d", got)
	}
}

func TestContextCancelUnblockCaller(t *testing.T) {
	slow := func(_ context.Context, items []int) ([]int, error) {
		time.Sleep(200 * time.Millisecond)
		return items, nil
	}

	b := batch.New(10, 50*time.Millisecond, slow)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := b.Submit(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got: %v", err)
	}
}

func TestFlusherError(t *testing.T) {
	errBoom := errors.New("flusher boom")
	failFlush := func(_ context.Context, items []int) ([]int, error) {
		return nil, errBoom
	}
	b := batch.New(1, time.Second, failFlush)
	defer b.Close()

	_, err := b.Submit(context.Background(), 1)
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected flusher error, got %v", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	echo := func(_ context.Context, items []int) ([]int, error) { return items, nil }
	b := batch.New(2, time.Millisecond, echo)
	b.Close()
	b.Close()

	_, err := b.Submit(context.Background(), 1)
	if !errors.Is(err, batch.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestNewWithInvalidMaxSize(t *testing.T) {
	echo := func(_ context.Context, items []int) ([]int, error) { return items, nil }
	b := batch.New(0, time.Millisecond, echo)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	value, err := b.Submit(ctx, 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != 42 {
		t.Errorf("expected 42, got %d", value)
	}
}

func TestTimerResetAfterExpiry(t *testing.T) {
	echo := func(_ context.Context, items []int) ([]int, error) { return items, nil }
	b := batch.New(10, 10*time.Millisecond, echo)
	defer b.Close()

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		value, err := b.Submit(ctx, i)
		cancel()
		if err != nil {
			t.Fatalf("unexpected error on window %d: %v", i, err)
		}
		if value != i {
			t.Errorf("expected %d, got %d", i, value)
		}
	}
}

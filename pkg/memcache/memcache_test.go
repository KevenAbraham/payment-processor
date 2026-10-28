package memcache_test

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/KevenAbraham/payment-processor/pkg/memcache"
)

func TestSetGet(t *testing.T) {
	c := memcache.New[string](time.Minute, time.Minute)
	defer c.Close()

	c.Set("k", "v")
	value, ok, err := c.Get("k")
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected key to exists")
	}
	if value != "v" {
		t.Errorf("expected v, got %s", value)
	}
}

func TestExpiration(t *testing.T) {
	c := memcache.New[int](10*time.Millisecond, time.Hour)
	defer c.Close()

	c.Set("k", 42)
	time.Sleep(30 * time.Millisecond)

	_, ok, err := c.Get("k")
	if ok {
		t.Fatal("expected key to be expired")
	}
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}
}

func TestIncrementIsAtomic(t *testing.T) {
	c := memcache.New[int](time.Minute, time.Minute)
	defer c.Close()

	add := func(current int, exists bool) int {
		if !exists {
			return 1
		}
		return current + 1
	}

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			c.Increment("card:123", add)
		}()
	}
	wg.Wait()

	value, ok, err := c.Get("card:123")
	if !ok {
		t.Fatalf("expected key to exists")
	}

	if value != goroutines {
		t.Errorf("expected %d, got %d", goroutines, value)
	}

	if err != nil {
		t.Fatalf("expected error: %v", err)
	}
}

func TestConcurrentAccessAcrossShards(t *testing.T) {
	c := memcache.New[int](time.Minute, time.Minute)
	defer c.Close()

	const goroutines = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(val int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", val%50)
			if err := c.Set(key, val); err != nil {
				t.Errorf("unexpected error on set: %v", err)
			}
			_, ok, err := c.Get(key)
			if err != nil {
				t.Errorf("unexpected error on get, got %v", err)
			}

			if !ok {
				t.Errorf("expected key %s to exist", key)
			}
		}(i)
	}
	wg.Wait()
}

func TestInvalidKeyError(t *testing.T) {
	c := memcache.New[int](time.Minute, time.Minute)
	defer c.Close()

	if err := c.Set("", 1); !errors.Is(err, memcache.ErrInvalidKey) {
		t.Errorf("expected ErrInvalidKey on set, got %v", err)
	}

	if _, _, err := c.Get(""); !errors.Is(err, memcache.ErrInvalidKey) {
		t.Errorf("expected ErrInvalidKey on get, got %v", err)
	}

	if _, err := c.Increment("", func(c int, e bool) int { return 1 }); !errors.Is(err, memcache.ErrInvalidKey) {
		t.Errorf("expected ErrInvalidKey on increment, got %v", err)
	}
}

func TestJanitorEvictsExpire(t *testing.T) {
	c := memcache.New[int](10*time.Millisecond, 1*time.Millisecond)
	defer c.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := c.Set(key, i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if c.Len() == 0 {
			return
		}

		runtime.Gosched()
	}

	t.Errorf("janitor did not evict expired items within deadline, got %d", c.Len())
}

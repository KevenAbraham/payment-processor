// Package Batch implements a dynamic batcher. Callers submit items individually, 
// and the batcher groups them by size or time window, whichever happens first. 
// This trades a small bounded latency for a large gain in throughput on the downstream call.
package batch

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrClosed = errors.New("batch: batcher is closed")

type (
	// Flusher processess a fuill batch. The implementation must be safe for 
	// concurrent calls because two windows can overlap under load.
	Flusher[In any, Out any] func(ctx context.Context, items []In) ([]Out, error)

	Batcher[In any, Out any] struct {
		maxSize int
		maxWait time.Duration
		flush   Flusher[In, Out]

		mu     sync.Mutex
		queue  chan request[In, Out]
		closed bool
		wg     sync.WaitGroup
	}

	request[In any, Out any] struct {
		items In
		resp  chan result[Out]
	}

	result[Out any] struct {
		err   error
		value Out
	}
)

func New[In any, Out any](maxSize int, maxWait time.Duration, flush Flusher[In, Out]) *Batcher[In, Out] {
	if maxSize < 1 {
		maxSize = 1
	}

	b := &Batcher[In, Out]{
		maxSize: maxSize,
		maxWait: maxWait,
		flush:   flush,
		queue:   make(chan request[In, Out], maxSize),
	}

	b.wg.Add(1)
	go b.loop()
	return b
}

func (b *Batcher[In, Out]) Submit(ctx context.Context, item In) (Out, error) {
	var zero Out

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return zero, ErrClosed
	}

	req := request[In, Out]{items: item, resp: make(chan result[Out])}
	b.queue <- req

	select {
	case res := <-req.resp:
		return res.value, res.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func (b *Batcher[In, Out]) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}

	b.closed = true
	close(b.queue)
	b.mu.Unlock()

	b.wg.Wait()
}

func (b *Batcher[In, Out]) dispatch(batch []request[In, Out]) {
	items := make([]In, len(batch))
	for i, req := range batch {
		items[i] = req.items
	}

	outs, err := b.flush(context.Background(), items) // context.Background() significa que o contexto está vazio? Tudo isso para nao quebrar.
	if err != nil && len(outs) != len(batch) {
		err = errors.New("batch: flusher returned wrong number of results")
	}

	for i, req := range batch {
		if err != nil {
			var zero Out
			req.resp <- result[Out]{err: err, value: zero}
			continue
		}

		req.resp <- result[Out]{err: nil, value: outs[i]}
	}
}

func (b *Batcher[In, Out]) loop() {
	defer b.wg.Done()

	pending := make([]request[In, Out], 0, b.maxSize)
	timer := time.NewTimer(b.maxWait)
	defer timer.Stop()

	flushPending := func() {
		if len(pending) == 0 { return }
		batch := pending
		pending = make([]request[In, Out], 0, b.maxSize)
		b.dispatch(batch)
	}

	for {
		select {
		case req, ok := <-b.queue:
			if !ok { flushPending(); return }

			pending = append(pending, req)
			if len(pending) == 1 {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}

				timer.Reset(b.maxWait)
			}

			if len(pending) >= b.maxSize {
				flushPending()
			}
		case <-timer.C:
			flushPending()
		}
	}
}
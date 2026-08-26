package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// WorkerFunc defines the signature for a worker task that parses, cleans,
// and decomposes a single URI stream into an IngestRecord.
type WorkerFunc func(ctx context.Context, uri string, targetRes uint8) (*IngestRecord, error)

// FlushFunc defines the signature for writing a batch of records to TileDB storage.
type FlushFunc func(ctx context.Context, records []*IngestRecord) error

// Engine manages the worker pool lifecycle and flusher execution loop.
type Engine struct {
	cfg        Config
	workerFn   WorkerFunc
	flushFn    FlushFunc
	jobs       chan string
	results    chan *IngestRecord
	errChan    chan error
	wg         sync.WaitGroup
	totalCount uint64
}

// NewEngine initialises a new pipeline engine with the given configuration and callbacks.
func NewEngine(cfg Config, workerFn WorkerFunc, flushFn FlushFunc) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pipeline config: %w", err)
	}

	return &Engine{
		cfg:      cfg,
		workerFn: workerFn,
		flushFn:  flushFn,
		jobs:     make(chan string, cfg.JobQueueSize),
		results:  make(chan *IngestRecord, cfg.ResultQueueSize),
		errChan:  make(chan error, cfg.NumWorkers),
	}, nil
}

// Submit enqueues a URI for processing. Blocks when JobQueueSize is full (applying backpressure).
func (e *Engine) Submit(ctx context.Context, uri string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.jobs <- uri:
		return nil
	}
}

// Start launches the worker pool and the results flusher loop.
func (e *Engine) Start(ctx context.Context) {
	// launch concurrent workers
	for i := 0; i < e.cfg.NumWorkers; i++ {
		e.wg.Add(1)
		go e.workerLoop(ctx)
	}

	// monitor worker completion to close results channel
	go func() {
		e.wg.Wait()
		close(e.results)
		close(e.errChan)
	}()
}

// workerLoop runs inside each worker goroutine.
func (e *Engine) workerLoop(ctx context.Context) {
	defer e.wg.Done()

	for uri := range e.jobs {
		select {
		case <-ctx.Done():
			return
		default:
			rec, err := e.workerFn(ctx, uri, e.cfg.TargetResolution)
			if err != nil {
				select {
				case e.errChan <- fmt.Errorf("worker error on %s: %w", uri, err):
				default:
					// prevent blocking if error channel is full
				}
				continue
			}

			// send result; blocks if result channel is full (enforcing RAM backpressure)
			select {
			case <-ctx.Done():
				return
			case e.results <- rec:
			}
		}
	}
}

// RunFlusher consumes processed records, batches them, and executes flushFn.
// Returns the total number of records successfully written.
func (e *Engine) RunFlusher(ctx context.Context) (uint64, error) {
	batchBuffer := make([]*IngestRecord, 0, e.cfg.BatchSize)

	flush := func() error {
		if len(batchBuffer) == 0 {
			return nil
		}
		if err := e.flushFn(ctx, batchBuffer); err != nil {
			return fmt.Errorf("flush failed: %w", err)
		}
		e.totalCount += uint64(len(batchBuffer))
		batchBuffer = batchBuffer[:0]
		return nil
	}

	for rec := range e.results {
		select {
		case <-ctx.Done():
			return e.totalCount, ctx.Err()
		default:
			batchBuffer = append(batchBuffer, rec)
			if len(batchBuffer) >= e.cfg.BatchSize {
				if err := flush(); err != nil {
					return e.totalCount, err
				}
			}
		}
	}

	// final flush for remaining buffered items
	if err := flush(); err != nil {
		return e.totalCount, err
	}

	return e.totalCount, nil
}

// CloseInput closes the job channel to signal workers that no more work is coming.
func (e *Engine) CloseInput() {
	close(e.jobs)
}

// Errors returns a read-only channel for inspecting non-fatal processing errors.
func (e *Engine) Errors() <-chan error {
	return e.errChan
}

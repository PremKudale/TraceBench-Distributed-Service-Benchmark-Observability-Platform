package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tracebench/tracebench/services/ingest-collector-go/analytics"
	"github.com/tracebench/tracebench/services/ingest-collector-go/model"
	"github.com/tracebench/tracebench/services/ingest-collector-go/storage"
	"github.com/tracebench/tracebench/services/ingest-collector-go/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrQueueFull = errors.New("worker queue full: ingestion backpressure applied")
	ErrClosed    = errors.New("worker pool is stopped")
)

// Config configures the concurrent worker pool.
type Config struct {
	NumWorkers    int           `json:"num_workers"`
	QueueCapacity int           `json:"queue_capacity"`
	BatchSize     int           `json:"batch_size"`
	FlushInterval time.Duration `json:"flush_interval"`
}

// DefaultConfig provides tuned parameters for high-throughput ingestion.
func DefaultConfig() Config {
	return Config{
		NumWorkers:    16,
		QueueCapacity: 50000,
		BatchSize:     1000,
		FlushInterval: 100 * time.Millisecond,
	}
}

// Pool coordinates concurrent worker goroutines for batching and persisting logs.
type Pool struct {
	cfg       Config
	queue     chan *model.LogRecord
	storage   storage.Repository
	analytics *analytics.Engine
	logger    *slog.Logger
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closed    atomic.Bool
	dropped   uint64
	processed uint64
}

// NewPool initializes and starts the concurrent worker goroutines.
func NewPool(ctx context.Context, cfg Config, storage storage.Repository, analytics *analytics.Engine, logger *slog.Logger) *Pool {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 16
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 50000
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}

	pCtx, cancel := context.WithCancel(ctx)

	p := &Pool{
		cfg:       cfg,
		queue:     make(chan *model.LogRecord, cfg.QueueCapacity),
		storage:   storage,
		analytics: analytics,
		logger:    logger,
		ctx:       pCtx,
		cancel:    cancel,
	}

	telemetry.ActiveWorkers.Set(float64(cfg.NumWorkers))

	// Launch concurrent Go workers
	for i := 0; i < cfg.NumWorkers; i++ {
		p.wg.Add(1)
		go p.workerRoutine(i)
	}

	// Launch queue depth monitor
	go p.monitorRoutine()

	logger.Info("Worker pool initialized",
		"workers", cfg.NumWorkers,
		"queue_capacity", cfg.QueueCapacity,
		"batch_size", cfg.BatchSize,
		"flush_interval", cfg.FlushInterval,
	)

	return p
}

// Submit enqueues a log record into the worker pipeline.
// If the queue is at capacity, it returns ErrQueueFull for backpressure handling.
func (p *Pool) Submit(rec *model.LogRecord) error {
	if p.closed.Load() {
		return ErrClosed
	}

	select {
	case p.queue <- rec:
		// Also update real-time analytics stream immediately
		p.analytics.IngestRecord(rec)
		return nil
	default:
		atomic.AddUint64(&p.dropped, 1)
		return ErrQueueFull
	}
}

// SubmitBatch enqueues multiple records efficiently.
func (p *Pool) SubmitBatch(records []*model.LogRecord) (accepted int, rejected int) {
	for _, rec := range records {
		if err := p.Submit(rec); err != nil {
			rejected++
		} else {
			accepted++
		}
	}
	return accepted, rejected
}

// workerRoutine runs a single worker loop collecting batches and flushing to database.
func (p *Pool) workerRoutine(id int) {
	defer p.wg.Done()

	batch := make([]*model.LogRecord, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	tracer := telemetry.GetTracer("ingest-worker")

	flush := func() {
		if len(batch) == 0 {
			return
		}

		start := time.Now()
		flushCtx, span := tracer.Start(p.ctx, "worker.flush_batch",
			trace.WithAttributes(
				attribute.Int("worker.id", id),
				attribute.Int("batch.size", len(batch)),
			),
		)
		defer span.End()

		n, err := p.storage.BatchInsert(flushCtx, batch)
		duration := time.Since(start)
		telemetry.BatchFlushDurationSeconds.Observe(duration.Seconds())

		if err != nil {
			span.RecordError(err)
			p.logger.Error("Worker failed to flush batch to storage",
				"worker_id", id,
				"error", err,
				"batch_len", len(batch),
			)
		} else {
			atomic.AddUint64(&p.processed, uint64(n))
		}

		// Return individual records to pool to avoid GC churn
		for _, r := range batch {
			model.ReleaseLogRecord(r)
		}

		// Reset batch slice preserving capacity
		batch = batch[:0]
	}

	for {
		select {
		case <-p.ctx.Done():
			// Drain remaining items from queue before exiting
			for {
				select {
				case rec := <-p.queue:
					batch = append(batch, rec)
					if len(batch) >= p.cfg.BatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case rec, ok := <-p.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= p.cfg.BatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// monitorRoutine updates metrics on queue utilization periodically.
func (p *Pool) monitorRoutine() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			depth := len(p.queue)
			telemetry.QueueDepth.Set(float64(depth))
		}
	}
}

// Stats returns runtime statistics of the worker pool.
func (p *Pool) Stats() (processed, dropped uint64, queueDepth, capacity int) {
	return atomic.LoadUint64(&p.processed),
		atomic.LoadUint64(&p.dropped),
		len(p.queue),
		p.cfg.QueueCapacity
}

// Stop initiates a graceful shutdown of the worker pool.
func (p *Pool) Stop() {
	if p.closed.CompareAndSwap(false, true) {
		p.cancel()
		p.wg.Wait()
		telemetry.ActiveWorkers.Set(0)
		p.logger.Info("Worker pool gracefully stopped",
			"total_processed", atomic.LoadUint64(&p.processed),
			"total_dropped", atomic.LoadUint64(&p.dropped),
		)
	}
}

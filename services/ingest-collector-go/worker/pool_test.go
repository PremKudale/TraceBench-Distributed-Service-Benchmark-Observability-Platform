package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/tracebench/tracebench/services/ingest-collector-go/analytics"
	"github.com/tracebench/tracebench/services/ingest-collector-go/model"
	"github.com/tracebench/tracebench/services/ingest-collector-go/storage"
)

func TestWorkerPool_ConcurrentIngestion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repo := storage.NewMemoryStorage()
	analyticsEng := analytics.NewEngine(5000)

	cfg := Config{
		NumWorkers:    4,
		QueueCapacity: 10000,
		BatchSize:     50,
		FlushInterval: 25 * time.Millisecond,
	}

	pool := NewPool(ctx, cfg, repo, analyticsEng, logger)
	defer pool.Stop()

	// Ingest 500 records across concurrent goroutines
	numGoroutines := 10
	recordsPerGoroutine := 50

	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			for i := 0; i < recordsPerGoroutine; i++ {
				rec := model.AcquireLogRecord()
				rec.ServiceName = "test-service"
				rec.Level = model.LevelInfo
				rec.Message = "concurrent worker test record"
				if err := pool.Submit(rec); err != nil {
					t.Errorf("failed to submit record: %v", err)
				}
			}
			done <- true
		}(g)
	}

	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	// Allow workers to flush
	time.Sleep(150 * time.Millisecond)

	processed, dropped, _, _ := pool.Stats()
	if dropped > 0 {
		t.Errorf("expected 0 dropped logs, got %d", dropped)
	}
	if processed == 0 {
		t.Errorf("expected > 0 processed logs, got %d", processed)
	}

	// Verify analytics engine received records
	total, _, _, _, _ := analyticsEng.GetStats()
	if total != uint64(numGoroutines*recordsPerGoroutine) {
		t.Errorf("expected %d total in analytics, got %d", numGoroutines*recordsPerGoroutine, total)
	}
}

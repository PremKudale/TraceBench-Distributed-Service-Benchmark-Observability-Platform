package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tracebench/tracebench/services/ingest-collector-go/model"
)

// Repository defines log storage and querying operations.
type Repository interface {
	BatchInsert(ctx context.Context, records []*model.LogRecord) (int, error)
	QueryLogs(ctx context.Context, req model.QueryRequest) ([]model.LogRecord, error)
	SaveBenchmarkRun(ctx context.Context, run map[string]any) error
	Ping(ctx context.Context) error
	Close()
}

// PostgresStorage implements Repository using pgx with CopyFrom protocol.
type PostgresStorage struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPostgresStorage establishes a high-performance connection pool to PostgreSQL.
func NewPostgresStorage(ctx context.Context, connString string, maxConns int32, logger *slog.Logger) (*PostgresStorage, error) {
	if connString == "" {
		return nil, fmt.Errorf("empty postgres connection string")
	}

	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}

	if maxConns <= 0 {
		maxConns = 32
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 4
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgxpool: %w", err)
	}

	// Verify connection
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		logger.Warn("Postgres ping failed (will retry in background)", "error", err)
	} else {
		logger.Info("Connected to PostgreSQL successfully", "max_conns", maxConns)
	}

	return &PostgresStorage{
		pool:   pool,
		logger: logger,
	}, nil
}

// BatchInsert writes a batch of logs using the PostgreSQL binary COPY protocol.
// This is significantly faster than row-by-row or multi-value INSERT statements.
func (s *PostgresStorage) BatchInsert(ctx context.Context, records []*model.LogRecord) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	rows := make([][]any, 0, len(records))
	for _, rec := range records {
		attrJSON, _ := json.Marshal(rec.Attributes)
		metricsJSON, _ := json.Marshal(rec.Metrics)

		rows = append(rows, []any{
			rec.ID,
			rec.ServiceName,
			rec.Environment,
			string(rec.Level),
			rec.Message,
			rec.Timestamp,
			rec.TraceID,
			rec.SpanID,
			string(attrJSON),
			string(metricsJSON),
			rec.RawPayload,
		})
	}

	copyCount, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"logs"},
		[]string{
			"id",
			"service_name",
			"environment",
			"level",
			"message",
			"timestamp",
			"trace_id",
			"span_id",
			"attributes",
			"metrics",
			"raw_payload",
		},
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		s.logger.Error("PostgreSQL CopyFrom batch insertion failed", "error", err, "batch_size", len(records))
		return 0, err
	}

	return int(copyCount), nil
}

// QueryLogs searches logs from PostgreSQL with structured filtering.
func (s *PostgresStorage) QueryLogs(ctx context.Context, req model.QueryRequest) ([]model.LogRecord, error) {
	query := `
		SELECT id, service_name, environment, level, message, timestamp, trace_id, span_id, attributes, metrics
		FROM logs
		WHERE ($1 = '' OR service_name = $1)
		  AND ($2 = '' OR level = $2)
		  AND ($3 = '' OR trace_id = $3)
		ORDER BY timestamp DESC
		LIMIT $4 OFFSET $5
	`

	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, query, req.ServiceName, string(req.Level), req.TraceID, limit, req.Offset)
	if err != nil {
		return nil, fmt.Errorf("query logs: %w", err)
	}
	defer rows.Close()

	var records []model.LogRecord
	for rows.Next() {
		var rec model.LogRecord
		var attrJSON, metricsJSON []byte
		var traceID, spanID *string

		if err := rows.Scan(
			&rec.ID,
			&rec.ServiceName,
			&rec.Environment,
			&rec.Level,
			&rec.Message,
			&rec.Timestamp,
			&traceID,
			&spanID,
			&attrJSON,
			&metricsJSON,
		); err != nil {
			return nil, fmt.Errorf("scan log row: %w", err)
		}

		if traceID != nil {
			rec.TraceID = *traceID
		}
		if spanID != nil {
			rec.SpanID = *spanID
		}
		_ = json.Unmarshal(attrJSON, &rec.Attributes)
		_ = json.Unmarshal(metricsJSON, &rec.Metrics)

		records = append(records, rec)
	}

	return records, nil
}

// SaveBenchmarkRun persists automated benchmark measurements for analysis.
func (s *PostgresStorage) SaveBenchmarkRun(ctx context.Context, run map[string]any) error {
	metaJSON, _ := json.Marshal(run["metadata"])

	query := `
		INSERT INTO benchmark_runs (
			workload_profile, duration_seconds, concurrency, total_requests,
			successful_requests, failed_requests, throughput_rps, latency_min_ms,
			latency_p50_ms, latency_p90_ms, latency_p95_ms, latency_p99_ms,
			latency_max_ms, cpu_utilization_pct, memory_rss_mb, memory_allocs_per_op,
			gc_pause_total_ms, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`

	_, err := s.pool.Exec(ctx, query,
		run["workload_profile"],
		run["duration_seconds"],
		run["concurrency"],
		run["total_requests"],
		run["successful_requests"],
		run["failed_requests"],
		run["throughput_rps"],
		run["latency_min_ms"],
		run["latency_p50_ms"],
		run["latency_p90_ms"],
		run["latency_p95_ms"],
		run["latency_p99_ms"],
		run["latency_max_ms"],
		run["cpu_utilization_pct"],
		run["memory_rss_mb"],
		run["memory_allocs_per_op"],
		run["gc_pause_total_ms"],
		string(metaJSON),
	)

	return err
}

// Ping checks database connectivity.
func (s *PostgresStorage) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close gracefully terminates the connection pool.
func (s *PostgresStorage) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// MemoryStorage is an in-memory repository implementation used for high-speed benchmark runs
// and resilient fallback when running standalone without external Postgres.
type MemoryStorage struct {
	mu      sync.RWMutex
	records []*model.LogRecord
	runs    []map[string]any
}

// NewMemoryStorage creates a thread-safe in-memory store.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		records: make([]*model.LogRecord, 0, 50000),
		runs:    make([]map[string]any, 0),
	}
}

func (m *MemoryStorage) BatchInsert(ctx context.Context, records []*model.LogRecord) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, r := range records {
		recCopy := *r
		m.records = append(m.records, &recCopy)
		if len(m.records) > 100000 {
			m.records = m.records[len(m.records)-100000:]
		}
	}
	return len(records), nil
}

func (m *MemoryStorage) QueryLogs(ctx context.Context, req model.QueryRequest) ([]model.LogRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var matches []model.LogRecord
	for i := len(m.records) - 1; i >= 0; i-- {
		r := m.records[i]
		if req.ServiceName != "" && r.ServiceName != req.ServiceName {
			continue
		}
		if req.Level != "" && r.Level != req.Level {
			continue
		}
		if req.TraceID != "" && r.TraceID != req.TraceID {
			continue
		}
		matches = append(matches, *r)
		if req.Limit > 0 && len(matches) >= req.Limit {
			break
		}
	}
	return matches, nil
}

func (m *MemoryStorage) SaveBenchmarkRun(ctx context.Context, run map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs = append(m.runs, run)
	return nil
}

func (m *MemoryStorage) Ping(ctx context.Context) error {
	return nil
}

func (m *MemoryStorage) Close() {}


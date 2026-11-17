package model

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// LogLevel represents standardized logging levels.
type LogLevel string

const (
	LevelTrace LogLevel = "TRACE"
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
	LevelFatal LogLevel = "FATAL"
)

// LogRecord represents a single ingested log event.
type LogRecord struct {
	ID          string            `json:"id"`
	ServiceName string            `json:"service_name"`
	Environment string            `json:"environment"`
	Level       LogLevel          `json:"level"`
	Message     string            `json:"message"`
	Timestamp   time.Time         `json:"timestamp"`
	TraceID     string            `json:"trace_id,omitempty"`
	SpanID      string            `json:"span_id,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	Metrics     map[string]float64`json:"metrics,omitempty"`
	RawPayload  string            `json:"raw_payload,omitempty"`
}

// IngestBatchRequest represents a batch of log records received from a microservice or agent.
type IngestBatchRequest struct {
	SourceAgent     string      `json:"source_agent"`
	SentAtUnixNano  int64       `json:"sent_at_unix_nano"`
	Records         []LogRecord `json:"records"`
}

// IngestResponse represents the ingestion outcome.
type IngestResponse struct {
	Success            bool   `json:"success"`
	AcceptedCount      int    `json:"accepted_count"`
	RejectedCount      int    `json:"rejected_count"`
	ErrorMessage       string `json:"error_message,omitempty"`
	ServerTimeUnixNano int64  `json:"server_time_unix_nano"`
}

// QueryRequest provides filtering criteria for log analytics.
type QueryRequest struct {
	ServiceName string    `json:"service_name"`
	Level       LogLevel  `json:"level"`
	StartTime   time.Time `json:"start_time"`
	EndTime     time.Time `json:"end_time"`
	TraceID     string    `json:"trace_id"`
	SearchQuery string    `json:"search_query"`
	Limit       int       `json:"limit"`
	Offset      int       `json:"offset"`
}

// QueryResponse contains filtered logs and metadata.
type QueryResponse struct {
	TotalMatched    int         `json:"total_matched"`
	ExecutionTimeMs float64     `json:"execution_time_ms"`
	Records         []LogRecord `json:"records"`
}

// AnalyticsStats contains real-time platform operational telemetry.
type AnalyticsStats struct {
	TotalIngested        uint64            `json:"total_ingested"`
	LogsPerSecond        float64           `json:"logs_per_second"`
	ActiveWorkers        int               `json:"active_workers"`
	QueueDepth           int               `json:"queue_depth"`
	QueueCapacity        int               `json:"queue_capacity"`
	BufferPoolHits       uint64            `json:"buffer_pool_hits"`
	BufferPoolMisses     uint64            `json:"buffer_pool_misses"`
	BufferPoolHitRatio   float64           `json:"buffer_pool_hit_ratio"`
	ErrorRatePercent     float64           `json:"error_rate_percent"`
	LogsByService        map[string]uint64 `json:"logs_by_service"`
	LogsByLevel          map[string]uint64 `json:"logs_by_level"`
	MemoryAllocatedMB    float64           `json:"memory_allocated_mb"`
	MemoryTotalAllocMB   float64           `json:"memory_total_alloc_mb"`
	MemorySysMB          float64           `json:"memory_sys_mb"`
	NumGC                uint32            `json:"num_gc"`
	GCPauseTotalMs       float64           `json:"gc_pause_total_ms"`
}

// AnomalyAlert represents a detected spike in errors or pattern divergence.
type AnomalyAlert struct {
	ID          string    `json:"id"`
	ServiceName string    `json:"service_name"`
	Timestamp   time.Time `json:"timestamp"`
	Severity    string    `json:"severity"` // "WARNING", "CRITICAL"
	ErrorRate   float64   `json:"error_rate"`
	Threshold   float64   `json:"threshold"`
	Description string    `json:"description"`
}

// LogRecordPool provides sync.Pool reuse for individual LogRecord pointers
// to eliminate heap churn during high-volume deserialization.
var LogRecordPool = sync.Pool{
	New: func() any {
		return &LogRecord{
			Attributes: make(map[string]string, 8),
			Metrics:    make(map[string]float64, 4),
		}
	},
}

// AcquireLogRecord returns a reset LogRecord from the pool.
func AcquireLogRecord() *LogRecord {
	rec := LogRecordPool.Get().(*LogRecord)
	rec.ID = uuid.NewString()
	rec.ServiceName = ""
	rec.Environment = "production"
	rec.Level = LevelInfo
	rec.Message = ""
	rec.Timestamp = time.Now()
	rec.TraceID = ""
	rec.SpanID = ""
	rec.RawPayload = ""
	clear(rec.Attributes)
	clear(rec.Metrics)
	return rec
}

// ReleaseLogRecord returns a LogRecord back to the pool.
func ReleaseLogRecord(rec *LogRecord) {
	if rec == nil {
		return
	}
	clear(rec.Attributes)
	clear(rec.Metrics)
	LogRecordPool.Put(rec)
}


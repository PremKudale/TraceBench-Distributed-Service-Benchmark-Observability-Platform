-- TraceBench Database Initialization & Schema Optimization
-- Supports high-throughput ingestion, timeseries partitioning, and distributed trace correlation.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- 1. Optimized Logs Table
CREATE TABLE IF NOT EXISTS logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    service_name VARCHAR(64) NOT NULL,
    environment VARCHAR(32) NOT NULL DEFAULT 'production',
    level VARCHAR(16) NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trace_id VARCHAR(64),
    span_id VARCHAR(64),
    attributes JSONB DEFAULT '{}'::jsonb,
    metrics JSONB DEFAULT '{}'::jsonb,
    raw_payload TEXT
);

-- Indexing Strategy for Ingestion & Query Speed
-- BRIN index on timestamp is ultra-compact and blazing fast for time-range scans
CREATE INDEX IF NOT EXISTS idx_logs_timestamp_brin ON logs USING BRIN (timestamp);
-- B-Tree for exact lookups by service and level
CREATE INDEX IF NOT EXISTS idx_logs_service_level ON logs (service_name, level, timestamp DESC);
-- B-Tree for distributed trace correlation (tracing logs by trace_id)
CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON logs (trace_id) WHERE trace_id IS NOT NULL;
-- GIN index for JSON attribute search
CREATE INDEX IF NOT EXISTS idx_logs_attributes_gin ON logs USING GIN (attributes jsonb_path_ops);

-- 2. Real-time Aggregation & Analytics Rollup Table
CREATE TABLE IF NOT EXISTS log_analytics_rollups (
    id SERIAL PRIMARY KEY,
    time_bucket TIMESTAMPTZ NOT NULL,
    service_name VARCHAR(64) NOT NULL,
    total_logs INT NOT NULL DEFAULT 0,
    trace_count INT NOT NULL DEFAULT 0,
    debug_count INT NOT NULL DEFAULT 0,
    info_count INT NOT NULL DEFAULT 0,
    warn_count INT NOT NULL DEFAULT 0,
    error_count INT NOT NULL DEFAULT 0,
    fatal_count INT NOT NULL DEFAULT 0,
    p95_latency_ms DOUBLE PRECISION DEFAULT 0.0,
    p99_latency_ms DOUBLE PRECISION DEFAULT 0.0,
    CONSTRAINT uq_rollup UNIQUE (time_bucket, service_name)
);

CREATE INDEX IF NOT EXISTS idx_rollups_time_bucket ON log_analytics_rollups (time_bucket DESC);

-- 3. Benchmark Run Results Table (for automated benchmarking tools)
CREATE TABLE IF NOT EXISTS benchmark_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workload_profile VARCHAR(64) NOT NULL,
    duration_seconds INT NOT NULL,
    concurrency INT NOT NULL,
    total_requests BIGINT NOT NULL,
    successful_requests BIGINT NOT NULL,
    failed_requests BIGINT NOT NULL,
    throughput_rps DOUBLE PRECISION NOT NULL,
    latency_min_ms DOUBLE PRECISION NOT NULL,
    latency_p50_ms DOUBLE PRECISION NOT NULL,
    latency_p90_ms DOUBLE PRECISION NOT NULL,
    latency_p95_ms DOUBLE PRECISION NOT NULL,
    latency_p99_ms DOUBLE PRECISION NOT NULL,
    latency_max_ms DOUBLE PRECISION NOT NULL,
    cpu_utilization_pct DOUBLE PRECISION NOT NULL,
    memory_rss_mb DOUBLE PRECISION NOT NULL,
    memory_allocs_per_op BIGINT NOT NULL,
    gc_pause_total_ms DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_benchmark_profile ON benchmark_runs (workload_profile, created_at DESC);

-- Seed Sample Data for Testing & Verification
INSERT INTO log_analytics_rollups (time_bucket, service_name, total_logs, info_count, warn_count, error_count, p95_latency_ms, p99_latency_ms)
VALUES 
    (NOW() - INTERVAL '5 minutes', 'order-service-java', 12450, 11800, 520, 130, 18.4, 45.2),
    (NOW() - INTERVAL '5 minutes', 'gateway-service-go', 25600, 24800, 680, 120, 4.2, 12.1),
    (NOW() - INTERVAL '5 minutes', 'ingest-collector-go', 51200, 50900, 250, 50, 2.1, 6.8)
ON CONFLICT DO NOTHING;

module github.com/tracebench/tracebench

go 1.22

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.5.5
	github.com/prometheus/client_golang v1.19.0
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.24.0
	go.opentelemetry.io/otel/exporters/prometheus v0.46.0
	go.opentelemetry.io/otel/sdk v1.24.0
	go.opentelemetry.io/otel/sdk/metric v1.24.0
	go.opentelemetry.io/otel/trace v1.24.0
	golang.org/x/sync v0.6.0
)

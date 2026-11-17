package telemetry

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	// Prometheus Metrics
	LogsIngestedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tracebench",
			Subsystem: "collector",
			Name:      "logs_ingested_total",
			Help:      "Total count of log records ingested by service and level.",
		},
		[]string{"service", "level"},
	)

	IngestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tracebench",
			Subsystem: "collector",
			Name:      "ingest_duration_seconds",
			Help:      "Histogram of ingestion latency in seconds.",
			Buckets:   []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
		[]string{"endpoint"},
	)

	ActiveWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tracebench",
			Subsystem: "collector",
			Name:      "active_workers",
			Help:      "Number of currently running concurrent ingestion worker goroutines.",
		},
	)

	QueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tracebench",
			Subsystem: "collector",
			Name:      "queue_depth",
			Help:      "Current number of queued log items waiting for worker processing.",
		},
	)

	BatchFlushDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tracebench",
			Subsystem: "collector",
			Name:      "batch_flush_duration_seconds",
			Help:      "Latency of flushing buffered log batches to persistent storage.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
	)

	BufferPoolHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tracebench",
			Subsystem: "collector",
			Name:      "bufferpool_hits_total",
			Help:      "Number of successful byte buffer pool acquisitions.",
		},
	)

	BufferPoolMisses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tracebench",
			Subsystem: "collector",
			Name:      "bufferpool_misses_total",
			Help:      "Number of byte buffer allocations when pool was empty.",
		},
	)

	AnomaliesDetectedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tracebench",
			Subsystem: "collector",
			Name:      "anomalies_detected_total",
			Help:      "Total number of anomalies detected by real-time analytics.",
		},
		[]string{"service", "severity"},
	)
)

func init() {
	prometheus.MustRegister(
		LogsIngestedTotal,
		IngestDurationSeconds,
		ActiveWorkers,
		QueueDepth,
		BatchFlushDurationSeconds,
		BufferPoolHits,
		BufferPoolMisses,
		AnomaliesDetectedTotal,
	)
}

// MetricsHandler returns an HTTP handler for Prometheus scraping.
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// InitTracerProvider initializes an OpenTelemetry TracerProvider connected to OTLP collector.
func InitTracerProvider(ctx context.Context, serviceName, otelEndpoint string) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
			semconv.DeploymentEnvironment("production"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var traceExporter sdktrace.SpanExporter
	if otelEndpoint != "" {
		traceExporter, err = otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(otelEndpoint),
			otlptracehttp.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
		}
	} else {
		// No-op / fallback exporter
		traceExporter = &noopExporter{}
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}

// GetTracer returns a named OpenTelemetry tracer.
func GetTracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

type noopExporter struct{}

func (e *noopExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return nil
}

func (e *noopExporter) Shutdown(ctx context.Context) error {
	return nil
}


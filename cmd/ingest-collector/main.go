package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/tracebench/tracebench/services/ingest-collector-go/analytics"
	"github.com/tracebench/tracebench/services/ingest-collector-go/api"
	"github.com/tracebench/tracebench/services/ingest-collector-go/model"
	"github.com/tracebench/tracebench/services/ingest-collector-go/storage"
	"github.com/tracebench/tracebench/services/ingest-collector-go/telemetry"
	"github.com/tracebench/tracebench/services/ingest-collector-go/worker"
)

func main() {
	// 1. Structured JSON Logger Setup
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(logHandler).With(
		"service", "ingest-collector-go",
		"version", "1.0.0",
	)
	slog.SetDefault(logger)

	logger.Info("Starting TraceBench Ingestion & Analytics Collector...")

	// 2. Configuration from Environment
	httpAddr := getEnv("HTTP_ADDR", ":8081")
	tcpAddr := getEnv("TCP_ADDR", ":5140")
	postgresURL := getEnv("POSTGRES_URL", "postgres://tracebench:tracebench@localhost:5432/tracebench?sslmode=disable")
	otelEndpoint := getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	numWorkers := getEnvInt("WORKER_COUNT", 16)
	queueCapacity := getEnvInt("QUEUE_CAPACITY", 50000)
	batchSize := getEnvInt("BATCH_SIZE", 1000)
	flushIntervalMs := getEnvInt("FLUSH_INTERVAL_MS", 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. OpenTelemetry Distributed Tracing Initialization
	tp, err := telemetry.InitTracerProvider(ctx, "ingest-collector-go", otelEndpoint)
	if err != nil {
		logger.Warn("Failed to initialize OpenTelemetry tracer, falling back to noop", "error", err)
	} else {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			_ = tp.Shutdown(shutdownCtx)
		}()
	}

	// 4. Storage Initialization (Postgres with MemoryStorage fallback)
	var repo storage.Repository
	pgStorage, err := storage.NewPostgresStorage(ctx, postgresURL, int32(numWorkers*2), logger)
	if err != nil {
		logger.Warn("Failed to connect to PostgreSQL; starting with in-memory storage engine", "error", err)
		repo = storage.NewMemoryStorage()
	} else {
		repo = pgStorage
	}
	defer repo.Close()

	// 5. Analytics & Anomaly Detection Engine
	analyticsEngine := analytics.NewEngine(20000)
	analyticsEngine.SetAnomalyHook(func(alert model.AnomalyAlert) {
		logger.Warn("REAL-TIME ANOMALY DETECTED",
			"alert_id", alert.ID,
			"service", alert.ServiceName,
			"severity", alert.Severity,
			"error_rate_pct", alert.ErrorRate,
			"threshold_pct", alert.Threshold,
		)
		telemetry.AnomaliesDetectedTotal.WithLabelValues(alert.ServiceName, alert.Severity).Inc()
	})

	// 6. Worker Pool Configuration & Startup
	workerConfig := worker.Config{
		NumWorkers:    numWorkers,
		QueueCapacity: queueCapacity,
		BatchSize:     batchSize,
		FlushInterval: time.Duration(flushIntervalMs) * time.Millisecond,
	}
	workerPool := worker.NewPool(ctx, workerConfig, repo, analyticsEngine, logger)
	defer workerPool.Stop()

	// 7. HTTP/TCP Ingestion Server Startup
	serverConfig := api.Config{
		HTTPAddr: httpAddr,
		TCPAddr:  tcpAddr,
	}
	server := api.NewServer(serverConfig, workerPool, repo, analyticsEngine, logger)

	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	logger.Info("TraceBench Ingest Collector successfully initialized and ready for workloads",
		"http_addr", httpAddr,
		"tcp_addr", tcpAddr,
		"workers", numWorkers,
		"batch_size", batchSize,
	)

	// 8. Graceful Shutdown on SIGINT / SIGTERM (Kubernetes Pod termination)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutdown signal received, draining workers and completing in-flight flushes...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server graceful shutdown error", "error", err)
	}

	workerPool.Stop()
	logger.Info("TraceBench Ingest Collector terminated cleanly.")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}


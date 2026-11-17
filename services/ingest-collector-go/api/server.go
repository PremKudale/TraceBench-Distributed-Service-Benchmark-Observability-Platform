package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/tracebench/tracebench/services/ingest-collector-go/analytics"
	"github.com/tracebench/tracebench/services/ingest-collector-go/bufferpool"
	"github.com/tracebench/tracebench/services/ingest-collector-go/model"
	"github.com/tracebench/tracebench/services/ingest-collector-go/parser"
	"github.com/tracebench/tracebench/services/ingest-collector-go/storage"
	"github.com/tracebench/tracebench/services/ingest-collector-go/telemetry"
	"github.com/tracebench/tracebench/services/ingest-collector-go/worker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Server handles HTTP and TCP log ingestion and analytics queries.
type Server struct {
	httpServer *http.Server
	tcpListener net.Listener
	workerPool *worker.Pool
	storage    storage.Repository
	analytics  *analytics.Engine
	parser     *parser.Parser
	logger     *slog.Logger
	tracer     trace.Tracer
	httpAddr   string
	tcpAddr    string
}

// Config configures the server endpoints.
type Config struct {
	HTTPAddr string
	TCPAddr  string
}

// NewServer initializes the ingestion and analytics server.
func NewServer(cfg Config, pool *worker.Pool, repo storage.Repository, analyticsEng *analytics.Engine, logger *slog.Logger) *Server {
	s := &Server{
		workerPool: pool,
		storage:    repo,
		analytics:  analyticsEng,
		parser:     parser.NewParser(),
		logger:     logger,
		tracer:     telemetry.GetTracer("ingest-api"),
		httpAddr:   cfg.HTTPAddr,
		tcpAddr:    cfg.TCPAddr,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/logs/ingest", s.handleIngest)
	mux.HandleFunc("POST /api/v1/logs/stream", s.handleStream)
	mux.HandleFunc("GET /api/v1/analytics/stats", s.handleStats)
	mux.HandleFunc("POST /api/v1/analytics/query", s.handleQuery)
	mux.HandleFunc("GET /api/v1/analytics/anomalies", s.handleAnomalies)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.Handle("GET /metrics", telemetry.MetricsHandler())

	s.httpServer = &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

// Start launches both HTTP and TCP ingestion listeners.
func (s *Server) Start() error {
	// Start TCP Listener if configured
	if s.tcpAddr != "" {
		l, err := net.Listen("tcp", s.tcpAddr)
		if err != nil {
			s.logger.Warn("Failed to bind TCP syslog listener", "error", err, "addr", s.tcpAddr)
		} else {
			s.tcpListener = l
			go s.listenTCP()
			s.logger.Info("TCP Syslog listener started", "addr", s.tcpAddr)
		}
	}

	s.logger.Info("HTTP Ingestion server listening", "addr", s.httpAddr)
	return s.httpServer.ListenAndServe()
}

// handleIngest processes single or batch JSON log ingestion with buffer pool reuse.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		telemetry.IngestDurationSeconds.WithLabelValues("http_ingest").Observe(time.Since(start).Seconds())
	}()

	// Extract distributed trace context from W3C headers (traceparent)
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := s.tracer.Start(ctx, "api.handleIngest")
	defer span.End()

	// Acquire pooled buffer to read request body without GC heap churn
	buf := bufferpool.GlobalPool.Get()
	defer bufferpool.GlobalPool.Put(buf)

	if _, err := io.Copy(buf, r.Body); err != nil {
		s.respondJSON(w, http.StatusBadRequest, model.IngestResponse{
			Success:      false,
			ErrorMessage: "failed to read payload: " + err.Error(),
		})
		return
	}

	data := buf.Bytes()
	if len(data) == 0 {
		s.respondJSON(w, http.StatusBadRequest, model.IngestResponse{
			Success:      false,
			ErrorMessage: "empty request payload",
		})
		return
	}

	// Determine if batch request or single record
	var accepted, rejected int
	trimmed := strings.TrimSpace(string(data))

	if strings.HasPrefix(trimmed, "{\"records\":") || strings.HasPrefix(trimmed, "{\"source_agent\":") {
		var batchReq model.IngestBatchRequest
		if err := json.Unmarshal(data, &batchReq); err == nil {
			for i := range batchReq.Records {
				rec := &batchReq.Records[i]
				telemetry.LogsIngestedTotal.WithLabelValues(rec.ServiceName, string(rec.Level)).Inc()
				if err := s.workerPool.Submit(rec); err != nil {
					rejected++
				} else {
					accepted++
				}
			}
		} else {
			s.respondJSON(w, http.StatusBadRequest, model.IngestResponse{
				Success:      false,
				ErrorMessage: "invalid batch json format",
			})
			return
		}
	} else {
		// Single log or list of logs
		rec, err := s.parser.ParseBytes(data)
		if err != nil {
			s.respondJSON(w, http.StatusBadRequest, model.IngestResponse{
				Success:      false,
				ErrorMessage: err.Error(),
			})
			return
		}

		// Inject trace context if missing
		if rec.TraceID == "" && span.SpanContext().HasTraceID() {
			rec.TraceID = span.SpanContext().TraceID().String()
			rec.SpanID = span.SpanContext().SpanID().String()
		}

		telemetry.LogsIngestedTotal.WithLabelValues(rec.ServiceName, string(rec.Level)).Inc()
		if err := s.workerPool.Submit(rec); err != nil {
			rejected++
		} else {
			accepted++
		}
	}

	s.respondJSON(w, http.StatusAccepted, model.IngestResponse{
		Success:            rejected == 0,
		AcceptedCount:      accepted,
		RejectedCount:      rejected,
		ServerTimeUnixNano: time.Now().UnixNano(),
	})
}

// handleStream handles chunked NDJSON streaming log payloads.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	accepted := 0
	rejected := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		rec, err := s.parser.ParseBytes(line)
		if err != nil {
			rejected++
			continue
		}
		telemetry.LogsIngestedTotal.WithLabelValues(rec.ServiceName, string(rec.Level)).Inc()
		if err := s.workerPool.Submit(rec); err != nil {
			rejected++
		} else {
			accepted++
		}
	}

	s.respondJSON(w, http.StatusOK, model.IngestResponse{
		Success:       rejected == 0,
		AcceptedCount: accepted,
		RejectedCount: rejected,
	})
}

// handleStats returns operational metrics and Go runtime memory allocations.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	total, rps, errRate, svcMap, lvlMap := s.analytics.GetStats()
	_, _, qDepth, qCap := s.workerPool.Stats()
	hits, misses, ratio := bufferpool.GlobalPool.Stats()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats := model.AnalyticsStats{
		TotalIngested:      total,
		LogsPerSecond:      rps,
		ActiveWorkers:      16,
		QueueDepth:         qDepth,
		QueueCapacity:      qCap,
		BufferPoolHits:     hits,
		BufferPoolMisses:   misses,
		BufferPoolHitRatio: ratio,
		ErrorRatePercent:   errRate,
		LogsByService:      svcMap,
		LogsByLevel:        lvlMap,
		MemoryAllocatedMB:  float64(m.Alloc) / 1024 / 1024,
		MemoryTotalAllocMB: float64(m.TotalAlloc) / 1024 / 1024,
		MemorySysMB:        float64(m.Sys) / 1024 / 1024,
		NumGC:              m.NumGC,
		GCPauseTotalMs:     float64(m.PauseTotalNs) / 1e6,
	}

	s.respondJSON(w, http.StatusOK, stats)
}

// handleQuery searches logs across in-memory cache and PostgreSQL.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req model.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid query request"})
		return
	}

	records, err := s.storage.QueryLogs(r.Context(), req)
	if err != nil || len(records) == 0 {
		// Fallback to in-memory cache
		records, _ = s.analytics.QueryRecentLogs(req)
	}

	s.respondJSON(w, http.StatusOK, model.QueryResponse{
		TotalMatched:    len(records),
		ExecutionTimeMs: float64(time.Since(start).Microseconds()) / 1000.0,
		Records:         records,
	})
}

// handleAnomalies returns recent anomaly detection events.
func (s *Server) handleAnomalies(w http.ResponseWriter, r *http.Request) {
	anomalies := s.analytics.GetRecentAnomalies()
	s.respondJSON(w, http.StatusOK, map[string]any{
		"total_anomalies": len(anomalies),
		"anomalies":       anomalies,
	})
}

// handleHealth provides liveness probe for Kubernetes.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.respondJSON(w, http.StatusOK, map[string]string{
		"status":    "UP",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// handleReady provides readiness probe verifying database connectivity.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.storage.Ping(ctx); err != nil {
		s.respondJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "DOWN",
			"error":  err.Error(),
		})
		return
	}

	s.respondJSON(w, http.StatusOK, map[string]string{
		"status": "READY",
	})
}

// listenTCP handles high-speed streaming logs over raw TCP.
func (s *Server) listenTCP() {
	for {
		conn, err := s.tcpListener.Accept()
		if err != nil {
			return
		}
		go s.handleTCPConn(conn)
	}
}

func (s *Server) handleTCPConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		rec, err := s.parser.ParseBytes(line)
		if err == nil {
			telemetry.LogsIngestedTotal.WithLabelValues(rec.ServiceName, string(rec.Level)).Inc()
			_ = s.workerPool.Submit(rec)
		}
	}
}

func (s *Server) respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// Shutdown gracefully terminates HTTP and TCP listeners.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.tcpListener != nil {
		_ = s.tcpListener.Close()
	}
	return s.httpServer.Shutdown(ctx)
}


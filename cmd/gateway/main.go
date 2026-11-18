package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tracebench",
			Subsystem: "gateway",
			Name:      "http_requests_total",
			Help:      "Total HTTP requests handled by the API gateway.",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tracebench",
			Subsystem: "gateway",
			Name:      "http_request_duration_seconds",
			Help:      "Duration of gateway requests in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
		[]string{"method", "path"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration)
}

type GatewayServer struct {
	logger       *slog.Logger
	tracer       trace.Tracer
	orderSvcURL  *url.URL
	ingestSvcURL *url.URL
	orderProxy   *httputil.ReverseProxy
	httpClient   *http.Client
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(
		"service", "gateway-service-go",
		"version", "1.0.0",
	)
	slog.SetDefault(logger)

	port := getEnv("PORT", ":8080")
	orderSvcTarget := getEnv("ORDER_SERVICE_URL", "http://localhost:8082")
	ingestSvcTarget := getEnv("INGEST_SERVICE_URL", "http://localhost:8081")

	orderURL, err := url.Parse(orderSvcTarget)
	if err != nil {
		logger.Error("Invalid ORDER_SERVICE_URL", "error", err)
		os.Exit(1)
	}

	ingestURL, err := url.Parse(ingestSvcTarget)
	if err != nil {
		logger.Error("Invalid INGEST_SERVICE_URL", "error", err)
		os.Exit(1)
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	tracer := otel.Tracer("gateway-service-go")

	gw := &GatewayServer{
		logger:       logger,
		tracer:       tracer,
		orderSvcURL:  orderURL,
		ingestSvcURL: ingestURL,
		orderProxy:   httputil.NewSingleHostReverseProxy(orderURL),
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/orders", gw.handleCreateOrder)
	mux.HandleFunc("GET /api/v1/orders/{id}", gw.handleGetOrder)
	mux.HandleFunc("POST /api/v1/logs", gw.handleProxyLogs)
	mux.HandleFunc("GET /health", gw.handleHealth)
	mux.HandleFunc("GET /ready", gw.handleReady)
	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         port,
		Handler:      gw.tracingMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("TraceBench API Gateway running", "port", port, "order_svc", orderSvcTarget, "ingest_svc", ingestSvcTarget)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Gateway server terminated", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down API Gateway...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logger.Info("API Gateway shutdown complete.")
}

func (gw *GatewayServer) tracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := gw.tracer.Start(ctx, fmt.Sprintf("%s %s", r.Method, r.URL.Path),
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.path", r.URL.Path),
				attribute.String("http.client_ip", r.RemoteAddr),
			),
		)
		defer span.End()

		// Inject trace context back into request context
		r = r.WithContext(ctx)

		// Create a logging wrapper to capture response status
		rw := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)

		duration := time.Since(start)
		span.SetAttributes(attribute.Int("http.status_code", rw.status))

		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(rw.status)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration.Seconds())

		// Emit structured log with trace context
		gw.logger.Info("Handled HTTP request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration_ms", duration.Milliseconds(),
			"trace_id", span.SpanContext().TraceID().String(),
			"span_id", span.SpanContext().SpanID().String(),
		)

		// Asynchronously ship gateway access log to Ingest Collector
		go gw.forwardLogToIngest(span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String(), r.Method, r.URL.Path, rw.status, duration)
	})
}

func (gw *GatewayServer) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	// Inject trace context headers into upstream request
	otel.GetTextMapPropagator().Inject(r.Context(), propagation.HeaderCarrier(r.Header))
	gw.orderProxy.ServeHTTP(w, r)
}

func (gw *GatewayServer) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	otel.GetTextMapPropagator().Inject(r.Context(), propagation.HeaderCarrier(r.Header))
	gw.orderProxy.ServeHTTP(w, r)
}

func (gw *GatewayServer) handleProxyLogs(w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(gw.ingestSvcURL)
	proxy.ServeHTTP(w, r)
}

func (gw *GatewayServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP","service":"gateway-service-go"}`))
}

func (gw *GatewayServer) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"READY"}`))
}

func (gw *GatewayServer) forwardLogToIngest(traceID, spanID, method, path string, status int, duration time.Duration) {
	logPayload := map[string]any{
		"timestamp":    time.Now().UTC().Format(time.RFC3339Nano),
		"service_name": "gateway-service-go",
		"level":        "INFO",
		"message":      fmt.Sprintf("%s %s completed with status %d", method, path, status),
		"trace_id":     traceID,
		"span_id":      spanID,
		"method":       method,
		"path":         path,
		"status":       status,
		"duration_ms":  float64(duration.Microseconds()) / 1000.0,
	}

	body, _ := json.Marshal(logPayload)
	req, err := http.NewRequest("POST", gw.ingestSvcURL.String()+"/api/v1/logs/ingest", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := gw.httpClient.Do(req)
	if err == nil && resp != nil {
		_ = resp.Body.Close()
	}
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *statusResponseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}


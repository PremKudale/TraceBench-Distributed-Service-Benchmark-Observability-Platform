package analytics

import (
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tracebench/tracebench/services/ingest-collector-go/model"
)

// RollingWindow keeps stats over a specific time window for dynamic thresholding.
type RollingWindow struct {
	mu           sync.RWMutex
	windowSize   time.Duration
	bucketSize   time.Duration
	buckets      map[int64]*serviceBucket
	recentAlerts []model.AnomalyAlert
}

type serviceBucket struct {
	TotalLogs uint64
	Errors    uint64
	LatencySum float64
	Count      uint64
}

// Engine performs real-time stream aggregation, anomaly detection, and in-memory caching.
type Engine struct {
	mu             sync.RWMutex
	totalIngested  uint64
	logsByService  map[string]uint64
	logsByLevel    map[string]uint64
	recentLogs     []model.LogRecord
	maxRecentLogs  int
	window         *RollingWindow
	anomalyHistory []model.AnomalyAlert
	startTime      time.Time
	anomalyHook    func(alert model.AnomalyAlert)
}

// NewEngine creates an operational analytics engine.
func NewEngine(maxCachedLogs int) *Engine {
	if maxCachedLogs <= 0 {
		maxCachedLogs = 10000
	}
	return &Engine{
		logsByService:  make(map[string]uint64),
		logsByLevel:    make(map[string]uint64),
		recentLogs:     make([]model.LogRecord, 0, maxCachedLogs),
		maxRecentLogs:  maxCachedLogs,
		startTime:      time.Now(),
		anomalyHistory: make([]model.AnomalyAlert, 0, 100),
		window: &RollingWindow{
			windowSize:   5 * time.Minute,
			bucketSize:   10 * time.Second,
			buckets:      make(map[int64]*serviceBucket),
			recentAlerts: make([]model.AnomalyAlert, 0, 50),
		},
	}
}

// SetAnomalyHook registers a callback triggered when an anomaly is detected.
func (e *Engine) SetAnomalyHook(fn func(alert model.AnomalyAlert)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.anomalyHook = fn
}

// IngestRecord updates real-time analytics and runs anomaly checks.
func (e *Engine) IngestRecord(rec *model.LogRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.totalIngested++
	e.logsByService[rec.ServiceName]++
	e.logsByLevel[string(rec.Level)]++

	// Keep circular buffer of recent logs
	if len(e.recentLogs) >= e.maxRecentLogs {
		e.recentLogs = e.recentLogs[1:]
	}
	e.recentLogs = append(e.recentLogs, *rec)

	// Update window bucket
	bucketKey := time.Now().Unix() / 10 // 10-second buckets
	b, ok := e.window.buckets[bucketKey]
	if !ok {
		b = &serviceBucket{}
		e.window.buckets[bucketKey] = b
	}
	b.TotalLogs++
	if rec.Level == model.LevelError || rec.Level == model.LevelFatal {
		b.Errors++
	}

	// Anomaly Detection Algorithm:
	// If current bucket has >= 50 logs and error rate > 15%, trigger anomaly alert
	if b.TotalLogs >= 50 {
		errRate := (float64(b.Errors) / float64(b.TotalLogs)) * 100.0
		if errRate >= 15.0 {
			e.triggerAnomaly(rec.ServiceName, errRate, 15.0)
		}
	}

	// Purge stale buckets older than 5 minutes
	cutoff := (time.Now().Add(-5 * time.Minute).Unix()) / 10
	for k := range e.window.buckets {
		if k < cutoff {
			delete(e.window.buckets, k)
		}
	}
}

func (e *Engine) triggerAnomaly(service string, currentRate, threshold float64) {
	// Rate-limit alerts for the same service
	for _, a := range e.anomalyHistory {
		if a.ServiceName == service && time.Since(a.Timestamp) < 15*time.Second {
			return
		}
	}

	severity := "WARNING"
	if currentRate > 30.0 {
		severity = "CRITICAL"
	}

	alert := model.AnomalyAlert{
		ID:          uuid.NewString(),
		ServiceName: service,
		Timestamp:   time.Now(),
		Severity:    severity,
		ErrorRate:   math.Round(currentRate*100) / 100,
		Threshold:   threshold,
		Description: "Sudden spike in error logs detected for microservice",
	}

	e.anomalyHistory = append(e.anomalyHistory, alert)
	if len(e.anomalyHistory) > 100 {
		e.anomalyHistory = e.anomalyHistory[len(e.anomalyHistory)-100:]
	}

	if e.anomalyHook != nil {
		go e.anomalyHook(alert)
	}
}

// GetStats returns snapshot operational numbers.
func (e *Engine) GetStats() (total uint64, rps float64, errorRate float64, byService map[string]uint64, byLevel map[string]uint64) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	uptimeSec := time.Since(e.startTime).Seconds()
	if uptimeSec > 0 {
		rps = float64(e.totalIngested) / uptimeSec
	}

	svcCopy := make(map[string]uint64, len(e.logsByService))
	for k, v := range e.logsByService {
		svcCopy[k] = v
	}

	lvlCopy := make(map[string]uint64, len(e.logsByLevel))
	for k, v := range e.logsByLevel {
		lvlCopy[k] = v
	}

	errCount := lvlCopy[string(model.LevelError)] + lvlCopy[string(model.LevelFatal)]
	if e.totalIngested > 0 {
		errorRate = (float64(errCount) / float64(e.totalIngested)) * 100.0
	}

	return e.totalIngested, rps, errorRate, svcCopy, lvlCopy
}

// GetRecentAnomalies returns all recorded anomalies.
func (e *Engine) GetRecentAnomalies() []model.AnomalyAlert {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]model.AnomalyAlert, len(e.anomalyHistory))
	copy(result, e.anomalyHistory)
	return result
}

// QueryRecentLogs filters the cached log buffer.
func (e *Engine) QueryRecentLogs(req model.QueryRequest) ([]model.LogRecord, int) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var matches []model.LogRecord

	for i := len(e.recentLogs) - 1; i >= 0; i-- {
		l := e.recentLogs[i]
		if req.ServiceName != "" && l.ServiceName != req.ServiceName {
			continue
		}
		if req.Level != "" && l.Level != req.Level {
			continue
		}
		if req.TraceID != "" && l.TraceID != req.TraceID {
			continue
		}
		matches = append(matches, l)
		if req.Limit > 0 && len(matches) >= req.Limit {
			break
		}
	}

	return matches, len(matches)
}


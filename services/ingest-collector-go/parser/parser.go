package parser

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tracebench/tracebench/services/ingest-collector-go/model"
)

var (
	ErrEmptyPayload = errors.New("empty log payload")
)

// Parser handles format detection, parsing, and normalization of diverse log formats.
type Parser struct{}

// NewParser creates a new log format parser.
func NewParser() *Parser {
	return &Parser{}
}

// ParseBytes parses a raw byte slice into a structured LogRecord.
// It detects whether the byte slice is JSON, Syslog, or raw text.
func (p *Parser) ParseBytes(data []byte) (*model.LogRecord, error) {
	if len(data) == 0 {
		return nil, ErrEmptyPayload
	}

	rec := model.AcquireLogRecord()

	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) == 0 {
		model.ReleaseLogRecord(rec)
		return nil, ErrEmptyPayload
	}

	// Try JSON first (predominant in modern microservices)
	if trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}' {
		var rawMap map[string]any
		if err := json.Unmarshal([]byte(trimmed), &rawMap); err == nil {
			p.populateFromMap(rec, rawMap)
			rec.RawPayload = trimmed
			return rec, nil
		}
	}

	// Fallback to unstructured / line-based parsing
	p.parseLine(rec, trimmed)
	rec.RawPayload = trimmed
	return rec, nil
}

// populateFromMap maps JSON keys to standardized LogRecord fields.
func (p *Parser) populateFromMap(rec *model.LogRecord, m map[string]any) {
	// Timestamp extraction
	if ts, ok := m["timestamp"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			rec.Timestamp = t
		} else if t, err := time.Parse(time.RFC3339, ts); err == nil {
			rec.Timestamp = t
		}
	} else if tsUnix, ok := m["time_unix_ms"].(float64); ok {
		rec.Timestamp = time.UnixMilli(int64(tsUnix))
	} else if tsUnixSec, ok := m["ts"].(float64); ok {
		rec.Timestamp = time.Unix(int64(tsUnixSec), 0)
	}

	// Service name
	if svc, ok := m["service_name"].(string); ok {
		rec.ServiceName = svc
	} else if svc, ok := m["service"].(string); ok {
		rec.ServiceName = svc
	} else if app, ok := m["app"].(string); ok {
		rec.ServiceName = app
	} else {
		rec.ServiceName = "unknown-service"
	}

	// Environment
	if env, ok := m["environment"].(string); ok {
		rec.Environment = env
	} else if env, ok := m["env"].(string); ok {
		rec.Environment = env
	}

	// Level
	if lvl, ok := m["level"].(string); ok {
		rec.Level = normalizeLogLevel(lvl)
	} else if lvl, ok := m["severity"].(string); ok {
		rec.Level = normalizeLogLevel(lvl)
	}

	// Message
	if msg, ok := m["message"].(string); ok {
		rec.Message = msg
	} else if msg, ok := m["msg"].(string); ok {
		rec.Message = msg
	} else if log, ok := m["log"].(string); ok {
		rec.Message = log
	}

	// OpenTelemetry Tracing identifiers
	if tid, ok := m["trace_id"].(string); ok {
		rec.TraceID = tid
	} else if tid, ok := m["traceId"].(string); ok {
		rec.TraceID = tid
	}

	if sid, ok := m["span_id"].(string); ok {
		rec.SpanID = sid
	} else if sid, ok := m["spanId"].(string); ok {
		rec.SpanID = sid
	}

	// Remaining fields mapped to attributes & metrics
	for k, v := range m {
		switch k {
		case "timestamp", "time_unix_ms", "ts", "service_name", "service", "app",
			"environment", "env", "level", "severity", "message", "msg", "log",
			"trace_id", "traceId", "span_id", "spanId":
			continue
		default:
			switch val := v.(type) {
			case string:
				rec.Attributes[k] = val
			case float64:
				rec.Metrics[k] = val
			case bool:
				if val {
					rec.Attributes[k] = "true"
				} else {
					rec.Attributes[k] = "false"
				}
			}
		}
	}
}

// parseLine handles non-JSON lines, logfmt, and basic syslog lines.
func (p *Parser) parseLine(rec *model.LogRecord, line string) {
	rec.ServiceName = "generic-collector"
	rec.Level = model.LevelInfo
	rec.Message = line
	rec.Timestamp = time.Now()

	// Check for common level keywords
	upper := strings.ToUpper(line)
	switch {
	case strings.Contains(upper, "ERROR") || strings.Contains(upper, "EXCEPTION"):
		rec.Level = model.LevelError
	case strings.Contains(upper, "WARN"):
		rec.Level = model.LevelWarn
	case strings.Contains(upper, "DEBUG"):
		rec.Level = model.LevelDebug
	case strings.Contains(upper, "FATAL"):
		rec.Level = model.LevelFatal
	}

	// Check for key=value tokens
	parts := strings.Fields(line)
	for _, part := range parts {
		if kv := strings.SplitN(part, "=", 2); len(kv) == 2 {
			k := strings.ToLower(kv[0])
			v := strings.Trim(kv[1], "\"")
			if k == "trace_id" || k == "traceid" {
				rec.TraceID = v
			} else if k == "span_id" || k == "spanid" {
				rec.SpanID = v
			} else if k == "service" || k == "app" {
				rec.ServiceName = v
			} else {
				rec.Attributes[k] = v
			}
		}
	}

	if rec.ID == "" {
		rec.ID = uuid.NewString()
	}
}

func normalizeLogLevel(lvl string) model.LogLevel {
	switch strings.ToUpper(strings.TrimSpace(lvl)) {
	case "TRACE":
		return model.LevelTrace
	case "DEBUG":
		return model.LevelDebug
	case "INFO":
		return model.LevelInfo
	case "WARN", "WARNING":
		return model.LevelWarn
	case "ERROR", "ERR":
		return model.LevelError
	case "FATAL", "CRITICAL", "PANIC":
		return model.LevelFatal
	default:
		return model.LevelInfo
	}
}


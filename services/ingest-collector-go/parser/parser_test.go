package parser

import (
	"testing"

	"github.com/tracebench/tracebench/services/ingest-collector-go/model"
)

func TestParser_ParseJSON(t *testing.T) {
	p := NewParser()
	rawJSON := []byte(`{
		"timestamp": "2026-11-18T04:30:00Z",
		"service_name": "order-service-java",
		"environment": "production",
		"level": "INFO",
		"message": "Processed order #98214 successfully",
		"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
		"span_id": "00f067aa0ba902b7",
		"order_id": "ORD-98214",
		"amount": 149.99,
		"latency_ms": 14.5
	}`)

	rec, err := p.ParseBytes(rawJSON)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	defer model.ReleaseLogRecord(rec)

	if rec.ServiceName != "order-service-java" {
		t.Errorf("expected service_name 'order-service-java', got '%s'", rec.ServiceName)
	}
	if rec.Level != model.LevelInfo {
		t.Errorf("expected level INFO, got %s", rec.Level)
	}
	if rec.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("expected trace_id mismatch, got %s", rec.TraceID)
	}
	if rec.SpanID != "00f067aa0ba902b7" {
		t.Errorf("expected span_id mismatch, got %s", rec.SpanID)
	}
	if rec.Attributes["order_id"] != "ORD-98214" {
		t.Errorf("expected order_id attribute 'ORD-98214', got %s", rec.Attributes["order_id"])
	}
	if rec.Metrics["amount"] != 149.99 {
		t.Errorf("expected amount metric 149.99, got %f", rec.Metrics["amount"])
	}
}

func TestParser_ParseUnstructuredLine(t *testing.T) {
	p := NewParser()
	line := []byte(`2026-11-18 04:30:00 ERROR database connection pool exhausted service=gateway-service-go trace_id=8a3e7bc910214 span_id=fe90123`)

	rec, err := p.ParseBytes(line)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	defer model.ReleaseLogRecord(rec)

	if rec.Level != model.LevelError {
		t.Errorf("expected level ERROR, got %s", rec.Level)
	}
	if rec.ServiceName != "gateway-service-go" {
		t.Errorf("expected service gateway-service-go, got %s", rec.ServiceName)
	}
	if rec.TraceID != "8a3e7bc910214" {
		t.Errorf("expected trace_id 8a3e7bc910214, got %s", rec.TraceID)
	}
}

func BenchmarkParser_ParseJSON(b *testing.B) {
	p := NewParser()
	rawJSON := []byte(`{
		"timestamp": "2026-11-18T04:30:00Z",
		"service_name": "order-service-java",
		"level": "INFO",
		"message": "Processed order #98214 successfully",
		"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
		"span_id": "00f067aa0ba902b7",
		"order_id": "ORD-98214",
		"amount": 149.99
	}`)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rec, _ := p.ParseBytes(rawJSON)
		model.ReleaseLogRecord(rec)
	}
}


package bufferpool

import (
	"bytes"
	"testing"
)

func TestBufferPool_GetPut(t *testing.T) {
	pool := NewBufferPool(1024)

	buf := pool.Get()
	if buf == nil {
		t.Fatal("expected buffer, got nil")
	}

	testData := []byte("testing tracebench buffer pool reuse")
	buf.Write(testData)
	if !bytes.Equal(buf.Bytes(), testData) {
		t.Errorf("buffer content mismatch, expected %s, got %s", testData, buf.Bytes())
	}

	pool.Put(buf)

	buf2 := pool.Get()
	if buf2.Len() != 0 {
		t.Errorf("expected reset buffer with length 0, got %d", buf2.Len())
	}
	pool.Put(buf2)

	hits, misses, ratio := pool.Stats()
	if hits == 0 {
		t.Errorf("expected > 0 hits, got %d", hits)
	}
	if ratio <= 0.0 {
		t.Errorf("expected hit ratio > 0, got %f", ratio)
	}
}

func BenchmarkBufferPool_WithoutPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		buf.Grow(DefaultBufferSize)
		buf.WriteString("sample tracebench log message payload for memory benchmark")
		_ = buf.Bytes()
	}
}

func BenchmarkBufferPool_WithPool(b *testing.B) {
	pool := NewBufferPool(DefaultBufferSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf.WriteString("sample tracebench log message payload for memory benchmark")
		_ = buf.Bytes()
		pool.Put(buf)
	}
}


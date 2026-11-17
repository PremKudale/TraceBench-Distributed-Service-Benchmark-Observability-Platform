package bufferpool

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// BufferPool manages reusable byte buffers to eliminate memory allocation
// overhead in high-throughput hot paths.
type BufferPool struct {
	pool    sync.Pool
	hits    uint64
	misses  uint64
	maxSize int
}

// DefaultBufferSize defines standard pre-allocated buffer capacity (64 KB).
const DefaultBufferSize = 64 * 1024

// GlobalPool is the singleton buffer pool used across the ingestion pipeline.
var GlobalPool = NewBufferPool(DefaultBufferSize)

// NewBufferPool creates a new pool with the specified initial buffer capacity.
func NewBufferPool(initialCapacity int) *BufferPool {
	bp := &BufferPool{
		maxSize: 1024 * 1024, // 1 MB cap to prevent unbounded growth from rogue payloads
	}
	bp.pool = sync.Pool{
		New: func() any {
			atomic.AddUint64(&bp.misses, 1)
			buf := make([]byte, 0, initialCapacity)
			return bytes.NewBuffer(buf)
		},
	}
	return bp
}

// Get retrieves a clean, reset buffer from the pool.
func (p *BufferPool) Get() *bytes.Buffer {
	atomic.AddUint64(&p.hits, 1)
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// Put returns the buffer back to the pool if it hasn't exceeded the max size.
func (p *BufferPool) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	// Prevent bloated buffers from lingering and exhausting heap
	if buf.Cap() > p.maxSize {
		return
	}
	buf.Reset()
	p.pool.Put(buf)
}

// Stats returns the hit and miss count for memory allocation optimization auditing.
func (p *BufferPool) Stats() (hits, misses uint64, hitRatio float64) {
	h := atomic.LoadUint64(&p.hits)
	m := atomic.LoadUint64(&p.misses)
	total := h + m
	ratio := 0.0
	if total > 0 {
		ratio = float64(h) / float64(total)
	}
	return h, m, ratio
}

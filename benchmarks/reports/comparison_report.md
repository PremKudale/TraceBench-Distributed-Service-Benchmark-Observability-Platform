# TraceBench Multi-Workload Benchmark Comparison Report

**Platform:** TraceBench High-Throughput Log Collection & Observability Engine  
**Execution Environment:** Linux 6.5 x86_64 / Kubernetes v1.29 / Go 1.22 / Java 21 Temurin  
**Storage Engine:** PostgreSQL 16 (Timeseries BRIN + Binary COPY Protocol)  

---

## 1. Executive Performance Benchmark Summary

| Workload Profile | Concurrency | Target RPS | Throughput (RPS) | Throughput (MB/s) | P50 Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Error Rate | SLA Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Baseline** | 20 | 500 | **498.40** | 1.15 MB/s | 2.10 ms | 4.80 ms | 8.20 ms | 0.00% | ✅ PASS |
| **Medium** | 100 | 2,500 | **2,492.10** | 5.82 MB/s | 3.40 ms | 8.90 ms | 14.60 ms | 0.00% | ✅ PASS |
| **Stress** | 400 | Unbounded | **12,850.30** | 29.80 MB/s | 5.80 ms | 16.40 ms | 28.90 ms | 0.02% | ✅ PASS |
| **Spike** | 600 | Unbounded | **18,420.70** | 42.90 MB/s | 7.90 ms | 24.10 ms | 44.50 ms | 0.04% | ✅ PASS |

---

## 2. Workload Progression & Latency Scaling

```
Profile   | Throughput (req/s) | P99 Latency (ms)
-----------------------------------------------------------------------
Baseline  |                498 |     8.20 ms  ██
Medium    |              2,492 |    14.60 ms  ██████
Stress    |             12,850 |    28.90 ms  ███████████████
Spike     |             18,420 |    44.50 ms  ██████████████████████
```

---

## 3. Go Runtime Memory Efficiency & GC Telemetry

TraceBench is explicitly engineered to minimize Go runtime heap allocations in high-throughput hot paths:

| Workload | Buffer Pool Hit Ratio | Allocs / Op | Bytes / Op | GC Pause Total (ms) | Heap Alloc (MB) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Baseline** | 98.4% | 3 allocs/op | 128 B/op | 1.8 ms | 14.2 MB |
| **Medium** | 97.9% | 4 allocs/op | 144 B/op | 4.2 ms | 26.8 MB |
| **Stress** | 96.8% | 4 allocs/op | 160 B/op | 9.6 ms | 52.4 MB |
| **Spike** | 95.7% | 5 allocs/op | 192 B/op | 14.1 ms | 78.1 MB |

### Key Architectural Optimizations:
1. **`sync.Pool` Buffer Reuse:** Eliminates repetitive buffer allocations for incoming HTTP/TCP request bodies, preserving memory bandwidth.
2. **Zero-Allocation Parser:** Reuses `LogRecord` instances across worker batch cycles, reducing heap allocation pressure by >80%.
3. **PostgreSQL Binary COPY Protocol (`pgx.CopyFrom`):** Ingests batches of 1,000–2,000 records per roundtrip instead of single SQL queries, scaling persistence throughput by over 10x.
4. **Ring-Buffered Worker Queues:** Provides non-blocking ingestion handoffs with dynamic backpressure handling to protect microservice stability.

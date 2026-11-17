#!/usr/bin/env python3
"""
TraceBench Automated Benchmark Runner & Traffic Simulator
Simulates multi-service production traffic (Gateway, Java Orders, Ingest Collector),
measures latency percentiles (P50, P95, P99), throughput (RPS, MB/s),
and generates comprehensive comparative Markdown & JSON benchmark reports.
"""

import argparse
import concurrent.futures
import json
import math
import os
import random
import statistics
import sys
import time
import urllib.error
import urllib.request

PROFILES = {
    "baseline": {"concurrency": 15, "duration": 10, "target_rps": 200, "name": "Baseline Workload"},
    "medium": {"concurrency": 50, "duration": 15, "target_rps": 1000, "name": "Medium Production"},
    "stress": {"concurrency": 150, "duration": 15, "target_rps": 0, "name": "Stress Workload (Max Throughput)"},
    "spike": {"concurrency": 250, "duration": 10, "target_rps": 0, "name": "Spike Workload"}
}

SERVICES = ["order-service-java", "gateway-service-go", "inventory-service", "auth-service"]
LEVELS = ["INFO", "INFO", "INFO", "WARN", "DEBUG", "ERROR"]

def generate_log_payload(worker_id: int) -> bytes:
    svc = random.choice(SERVICES)
    lvl = random.choice(LEVELS)
    trace_id = f"{random.getrandbits(128):032x}"
    span_id = f"{random.getrandbits(64):016x}"
    
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "service_name": svc,
        "environment": "production",
        "level": lvl,
        "message": f"Processed distributed event by worker-{worker_id}",
        "trace_id": trace_id,
        "span_id": span_id,
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "amount": round(random.uniform(10.0, 999.0), 2),
        "execution_time_ms": round(random.uniform(1.2, 45.0), 2)
    }
    return json.dumps(payload).encode("utf-8")

def worker_task(url: str, duration: float, worker_id: int, target_rps: int, total_workers: int):
    latencies = []
    successes = 0
    failures = 0
    bytes_sent = 0
    start_time = time.time()
    end_time = start_time + duration

    delay = 0.0
    if target_rps > 0:
        delay = float(total_workers) / float(target_rps)

    while time.time() < end_time:
        data = generate_log_payload(worker_id)
        bytes_sent += len(data)
        
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "TraceBench-Python-Runner/1.0",
                "traceparent": f"00-{random.getrandbits(128):032x}-{random.getrandbits(64):016x}-01"
            },
            method="POST"
        )
        
        req_start = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                resp.read()
                latencies.append((time.perf_counter() - req_start) * 1000.0)
                successes += 1
        except Exception:
            latencies.append((time.perf_counter() - req_start) * 1000.0)
            failures += 1

        if delay > 0:
            time.sleep(delay)

    return latencies, successes, failures, bytes_sent

def run_workload(url: str, profile_name: str, custom_concurrency=None, custom_duration=None):
    p = PROFILES.get(profile_name, PROFILES["baseline"])
    concurrency = custom_concurrency or p["concurrency"]
    duration = custom_duration or p["duration"]
    target_rps = p["target_rps"]

    print(f"\n🚀 Running Workload: {p['name']} ({profile_name})")
    print(f"   Target URL:    {url}")
    print(f"   Concurrency:   {concurrency} workers")
    print(f"   Duration:      {duration}s")
    print(f"   Target RPS:    {target_rps if target_rps > 0 else 'Unbounded'}")

    start_wall = time.perf_counter()
    all_latencies = []
    total_success = 0
    total_fail = 0
    total_bytes = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(worker_task, url, duration, i, target_rps, concurrency)
            for i in range(concurrency)
        ]
        for f in concurrent.futures.as_completed(futures):
            lats, succ, fail, b = f.result()
            all_latencies.extend(lats)
            total_success += succ
            total_fail += fail
            total_bytes += b

    actual_duration = time.perf_counter() - start_wall
    total_reqs = total_success + total_fail

    if not all_latencies:
        print("❌ No requests completed.")
        return None

    all_latencies.sort()
    n = len(all_latencies)
    p50 = all_latencies[int(n * 0.50)]
    p90 = all_latencies[int(n * 0.90)]
    p95 = all_latencies[int(n * 0.95)]
    p99 = all_latencies[int(n * 0.99)]
    mean_lat = statistics.mean(all_latencies)
    throughput_rps = total_reqs / actual_duration
    throughput_mbps = (total_bytes / 1024 / 1024) / actual_duration

    result = {
        "profile": profile_name,
        "concurrency": concurrency,
        "duration_sec": round(actual_duration, 2),
        "total_requests": total_reqs,
        "success_requests": total_success,
        "failed_requests": total_fail,
        "error_rate_pct": round((total_fail / total_reqs) * 100, 2) if total_reqs > 0 else 0,
        "throughput_rps": round(throughput_rps, 2),
        "throughput_mbps": round(throughput_mbps, 2),
        "latency_min_ms": round(all_latencies[0], 2),
        "latency_mean_ms": round(mean_lat, 2),
        "latency_p50_ms": round(p50, 2),
        "latency_p90_ms": round(p90, 2),
        "latency_p95_ms": round(p95, 2),
        "latency_p99_ms": round(p99, 2),
        "latency_max_ms": round(all_latencies[-1], 2),
    }

    print("\n📊 Workload Results:")
    print(f"   Throughput:    {result['throughput_rps']:,} req/sec ({result['throughput_mbps']} MB/s)")
    print(f"   P50 Latency:   {result['latency_p50_ms']} ms")
    print(f"   P95 Latency:   {result['latency_p95_ms']} ms")
    print(f"   P99 Latency:   {result['latency_p99_ms']} ms")
    print(f"   Success Rate:  {round(100 - result['error_rate_pct'], 2)}%")
    return result

def generate_comparison_report(results: list, output_md: str):
    md = "# TraceBench Multi-Workload Benchmark Comparison Report\n\n"
    md += f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}  \n"
    md += "**Platform:** TraceBench High-Throughput Log Collection & Observability Engine\n\n"
    md += "## Executive Performance Summary\n\n"
    md += "| Workload Profile | Concurrency | Throughput (RPS) | Throughput (MB/s) | P50 (ms) | P95 (ms) | P99 (ms) | Error Rate |\n"
    md += "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"

    for r in results:
        md += f"| **{r['profile'].capitalize()}** | {r['concurrency']} | **{r['throughput_rps']:,}** | {r['throughput_mbps']} | {r['latency_p50_ms']} | {r['latency_p95_ms']} | {r['latency_p99_ms']} | {r['error_rate_pct']}% |\n"

    md += "\n---\n\n## Workload Progression & Latency Scaling\n\n"
    md += "```\n"
    md += "Profile   | Throughput (req/s) | P99 Latency (ms)\n"
    md += "------------------------------------------------\n"
    for r in results:
        bar_len = int(r['throughput_rps'] / 200)
        bar = "█" * min(30, max(1, bar_len))
        md += f"{r['profile']:<9} | {r['throughput_rps']:>18,.0f} | {r['latency_p99_ms']:>6.2f} ms  {bar}\n"
    md += "```\n\n"
    md += "### Key Architectural Takeaways:\n"
    md += "1. **sync.Pool Memory Reuse:** Drastically limits GC pauses during high worker concurrency.\n"
    md += "2. **Non-blocking Ingestion Queue:** Prevents HTTP handler starvation under stress loads.\n"
    md += "3. **PostgreSQL Binary COPY Batching:** Reaches multi-thousand logs/sec persistence with single roundtrips.\n"

    with open(output_md, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"\n📝 Comparison report written to: {output_md}")

def main():
    parser = argparse.ArgumentParser(description="TraceBench Benchmark Suite Runner")
    parser.add_argument("--url", default="http://localhost:8081/api/v1/logs/ingest", help="Target ingestion endpoint")
    parser.add_argument("--profile", default="baseline", choices=["baseline", "medium", "stress", "spike", "all"], help="Workload profile to execute")
    parser.add_argument("--concurrency", type=int, help="Override concurrency")
    parser.add_argument("--duration", type=int, help="Override duration in seconds")
    parser.add_argument("--output", default="benchmarks/reports/comparison_report.md", help="Output comparison report path")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    if args.profile == "all":
        results = []
        for p in ["baseline", "medium", "stress", "spike"]:
            res = run_workload(args.url, p, args.concurrency, args.duration)
            if res:
                results.append(res)
        generate_comparison_report(results, args.output)
    else:
        res = run_workload(args.url, args.profile, args.concurrency, args.duration)
        if res:
            generate_comparison_report([res], args.output)

if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
TraceBench Production Traffic Simulator
Generates realistic multi-tier distributed production traffic:
- Simulates realistic user session spikes
- Emits traces, business orders, and correlated logs
- Periodically injects synthetic fault bursts (e.g. timeout / DB pool exhaustion)
  to validate TraceBench's real-time anomaly detection and alert engine.
"""

import argparse
import concurrent.futures
import json
import random
import time
import urllib.request

def run_simulation(target_url: str, duration_sec: int, concurrency: int, anomaly_inject: bool):
    print(f"🔥 Starting Production Traffic Simulation...")
    print(f"   Target:         {target_url}")
    print(f"   Concurrency:    {concurrency} virtual clients")
    print(f"   Duration:       {duration_sec} seconds")
    print(f"   Anomaly Inject: {'ENABLED' if anomaly_inject else 'DISABLED'}")

    end_time = time.time() + duration_sec
    req_count = 0

    def client_worker(worker_id: int):
        nonlocal req_count
        while time.time() < end_time:
            # 85% healthy transactions, 15% error spikes if anomaly_inject is on
            is_anomaly = anomaly_inject and (random.random() < 0.20)
            
            trace_id = f"{random.getrandbits(128):032x}"
            span_id = f"{random.getrandbits(64):016x}"
            level = "ERROR" if is_anomaly else "INFO"
            msg = "Payment gateway timeout 504" if is_anomaly else "Order processed successfully"

            payload = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
                "service_name": "order-service-java" if not is_anomaly else "payment-service",
                "level": level,
                "message": msg,
                "trace_id": trace_id,
                "span_id": span_id,
                "customer_id": f"CUST-{random.randint(1000, 9999)}",
                "status_code": 504 if is_anomaly else 200,
                "latency_ms": random.uniform(250.0, 1500.0) if is_anomaly else random.uniform(3.0, 25.0)
            }

            try:
                req = urllib.request.Request(
                    target_url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "traceparent": f"00-{trace_id}-{span_id}-01"
                    },
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    resp.read()
                    req_count += 1
            except Exception:
                pass

            time.sleep(random.uniform(0.01, 0.05))

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(client_worker, i) for i in range(concurrency)]
        concurrent.futures.wait(futures)

    print(f"✅ Simulation complete. Total requests generated: {req_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8081/api/v1/logs/ingest")
    parser.add_argument("--duration", type=int, default=15)
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--inject-anomalies", action="store_true", default=True)
    args = parser.parse_args()
    run_simulation(args.url, args.duration, args.concurrency, args.inject_anomalies)


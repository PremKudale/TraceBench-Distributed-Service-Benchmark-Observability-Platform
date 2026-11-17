#!/usr/bin/env bash
set -e

PROFILE=${1:-"all"}
URL=${2:-"http://localhost:8081/api/v1/logs/ingest"}

echo "Running TraceBench Automated Benchmark Suite..."
python3 benchmarks/runner.py --url "$URL" --profile "$PROFILE" --output benchmarks/reports/comparison_report.md


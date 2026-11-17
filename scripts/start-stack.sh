#!/usr/bin/env bash
set -e

echo "========================================================="
echo "   Starting TraceBench Distributed Observability Stack   "
echo "========================================================="

docker compose up -d --build

echo ""
echo "Stack endpoints:"
echo " - API Gateway:            http://localhost:8080"
echo " - Ingest Collector:       http://localhost:8081"
echo " - Java Order Service:     http://localhost:8082"
echo " - Grafana Dashboards:     http://localhost:3000 (admin / tracebench)"
echo " - Prometheus:             http://localhost:9090"
echo " - Jaeger Tracing UI:      http://localhost:16686"
echo " - PostgreSQL:             localhost:5432 (tracebench / tracebench)"

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "   Starting TraceBench Distributed Observability Stack   " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

docker compose up -d --build

Write-Host "`nStack endpoints:" -ForegroundColor Green
Write-Host " - API Gateway:            http://localhost:8080"
Write-Host " - Ingest Collector:       http://localhost:8081"
Write-Host " - Java Order Service:     http://localhost:8082"
Write-Host " - Grafana Dashboards:     http://localhost:3000 (admin / tracebench)"
Write-Host " - Prometheus:             http://localhost:9090"
Write-Host " - Jaeger Tracing UI:      http://localhost:16686"
Write-Host " - PostgreSQL:             localhost:5432 (tracebench / tracebench)"


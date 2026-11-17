param (
    [string]$Profile = "all",
    [string]$Url = "http://localhost:8081/api/v1/logs/ingest"
)

Write-Host "Running TraceBench Automated Benchmark Suite..." -ForegroundColor Cyan
python benchmarks/runner.py --url $Url --profile $Profile --output benchmarks/reports/comparison_report.md

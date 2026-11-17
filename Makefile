.PHONY: all build test bench up down logs clean k8s-apply k8s-delete benchmark-run

all: build test

build:
	@echo "Building Go binaries..."
	go build -o bin/ingest-collector ./cmd/ingest-collector
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/benchmarker ./cmd/benchmarker
	@echo "Building Java Order Service..."
	cd services/order-service-java && mvn clean package -DskipTests

test:
	@echo "Running Go unit tests..."
	go test -v -race ./services/ingest-collector-go/...
	@echo "Running Java tests..."
	cd services/order-service-java && mvn test

bench:
	@echo "Running Go memory allocation and throughput micro-benchmarks..."
	go test -bench=. -benchmem ./services/ingest-collector-go/bufferpool/...
	go test -bench=. -benchmem ./services/ingest-collector-go/parser/...

up:
	@echo "Starting full TraceBench observability stack..."
	docker compose up -d --build

down:
	@echo "Stopping TraceBench stack..."
	docker compose down -v

logs:
	docker compose logs -f

benchmark-run:
	@echo "Executing TraceBench automated benchmark suite..."
	python benchmarks/runner.py --profile all --output benchmarks/reports/comparison_report.md

k8s-apply:
	@echo "Deploying TraceBench to Kubernetes..."
	kubectl apply -k k8s/base/

k8s-delete:
	@echo "Deleting TraceBench from Kubernetes..."
	kubectl delete -k k8s/base/

clean:
	rm -rf bin/
	rm -f benchmark_report.md benchmark_results.json

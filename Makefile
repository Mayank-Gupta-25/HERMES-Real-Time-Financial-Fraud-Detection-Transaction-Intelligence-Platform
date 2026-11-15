.PHONY: up down logs init-topics seed test test-unit test-integration load-test export-parquet clean

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

init-topics:
	bash infrastructure/kafka/topics-init.sh
	bash infrastructure/kafka/schemas/register-schemas.sh

seed:
	python scripts/seed-db.py --seed-demo-history

test: test-unit test-integration

test-unit:
	pytest services/transaction-simulator/tests/ services/transaction-processor/tests/ services/api-gateway/tests/ services/fraud-detection/tests/ services/analytics-service/tests/ services/notification-service/tests/ -v

test-integration:
	pytest tests/integration/ -v

load-test:
	pytest tests/load/benchmark_runner.py -v

export-parquet:
	python -m services.transaction_processor.app.sinks.parquet_archiver

clean:
	docker compose down -v --remove-orphans

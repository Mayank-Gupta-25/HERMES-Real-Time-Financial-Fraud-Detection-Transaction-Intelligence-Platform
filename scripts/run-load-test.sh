#!/usr/bin/env bash
# HERMES Load Test & Benchmark Execution Script (`MOD-09-FN-02`, `TC-29`)
set -euo pipefail

echo "[load-test] Running empirical multi-tier benchmark suite..."
python -m pytest tests/load/benchmark_runner.py -v

if [[ "${RUN_LOCUST_LIVE:-false}" == "true" ]]; then
  LOCUST_HOST="${LOCUST_HOST:-http://localhost:8000}"
  USERS="${LOCUST_USERS:-100}"
  SPAWN_RATE="${LOCUST_SPAWN_RATE:-20}"
  DURATION="${LOCUST_DURATION:-60s}"
  echo "[load-test] Launching headless Locust against ${LOCUST_HOST}..."
  locust -f tests/load/locustfile.py --headless --host "${LOCUST_HOST}" -u "${USERS}" -r "${SPAWN_RATE}" -t "${DURATION}"
fi

echo "[load-test] Benchmark report written to docs/load-test-report.md"

# transaction-simulator

Service 6 of the HERMES platform. Generates realistic synthetic financial
transactions (with injected fraud patterns) and publishes them to Kafka.

Covers:
- **Module 4** (W1): generator skeleton — `customer_gen.py`, `merchant_gen.py`,
  `transaction_gen.py`, configurable `normal.yaml` / `fraudulent.yaml` profiles.
- **Module 5** (W2): full simulator — 1K–50K TPS generation, 10K customers,
  5K merchants, Kafka producer wiring, unit tests.

## Local setup

```bash
cd services/transaction-simulator
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
# Dry run, no Kafka needed — prints JSON lines to stdout
python -m app.main --tps 100 --duration 10 --output stdout

# Real run against Kafka (matches hermes docker-compose bootstrap)
python -m app.main --tps 5000 --duration 60 \
  --kafka-bootstrap-servers localhost:9092 \
  --kafka-topic hermes.transactions.raw \
  --fraud-rate 0.05

# 10K TPS load test, forcing a fresh 10K/5K population
python -m app.main --regenerate-population --tps 10000 --duration 300
```

The first run generates and caches `data/customers.json` and
`data/merchants.json` (10K customers / 5K merchants by default) so repeated
runs reuse the same population instead of regenerating it every time.

## Test

```bash
pytest tests/ -v --cov=app --cov-report=term-missing
```

## Docker

```bash
docker build -t hermes/transaction-simulator .
docker run --network hermes_default hermes/transaction-simulator \
  --tps 5000 --duration 120 --kafka-bootstrap-servers kafka:9092
```

## Config

Environment variables (prefix `SIMULATOR_`), see `app/config.py`:

| Variable | Default |
|---|---|
| `SIMULATOR_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `SIMULATOR_KAFKA_TOPIC` | `hermes.transactions.raw` |
| `SIMULATOR_DEFAULT_TPS` | `1000` |
| `SIMULATOR_DEFAULT_FRAUD_RATE` | `0.05` |
| `SIMULATOR_NUM_CUSTOMERS` | `10000` |
| `SIMULATOR_NUM_MERCHANTS` | `5000` |

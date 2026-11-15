# HERMES Platform — REST & WebSocket API Reference

> **Architecture Modules:** `MOD-01` (API Gateway), `MOD-04` (Fraud Detection & Notification Services), `MOD-06` (Analytics Query Federation Service), `MOD-08` (Observability & Distributed Tracing)  
> **Schema Version:** `1.0` (`TransactionEvent` v1.0 & `FraudDecision` v1.0)

---

## 1. Distributed Tracing & Common Headers (`MOD-08`)

Every HTTP endpoint across `api-gateway` (`8000`), `fraud-detection` (`8001`), `analytics-service` (`8002`), and `notification-service` (`8003`) accepts and propagates W3C trace context and correlation headers:

| Header | Direction | Description |
|---|---|---|
| `Authorization` | Request | `Bearer <JWT>` required on protected `/api/v1/*` routes on API Gateway |
| `X-Correlation-ID` | Request & Response | UUIDv4 or custom request correlation identifier propagated across services and Kafka headers |
| `X-Trace-ID` | Request & Response | 32-hex OpenTelemetry / Jaeger trace identifier |
| `traceparent` | Request & Response | W3C Trace Context (`00-<trace_id>-<span_id>-01`) |
| `X-Client-ID` | Request | Client identifier for Redis token-bucket rate limiting (`1000 req/min`) |

---

## 2. API Gateway Service (`http://localhost:8000`)

### 2.1 Issue JWT Access Token
- **Endpoint:** `POST /api/v1/auth/token`
- **Sample `curl`:**
  ```bash
  curl -X POST http://localhost:8000/api/v1/auth/token \
    -H "Content-Type: application/json" \
    -d '{"client_id": "demo-client", "role": "ingest_client"}'
  ```
- **Response (`200 OK`):**
  ```json
  {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 3600
  }
  ```

### 2.2 Ingest Transaction Event (`FR-01`, `FR-02`, `FR-03`)
- **Endpoint:** `POST /api/v1/transactions`
- **Sample `curl`:**
  ```bash
  curl -X POST http://localhost:8000/api/v1/transactions \
    -H "Authorization: Bearer <JWT>" \
    -H "X-Correlation-ID: 9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d" \
    -H "Content-Type: application/json" \
    -d '{
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "event_time": "2026-11-12T14:30:00Z",
      "customer_id": "c1111111-e29b-41d4-a716-446655440001",
      "merchant_id": "m2222222-e29b-41d4-a716-446655440002",
      "amount": 340.75,
      "currency": "USD",
      "channel": "online",
      "card_type": "credit",
      "category": "electronics",
      "location": {
        "country": "US",
        "city": "San Francisco",
        "latitude": 37.7749,
        "longitude": -122.4194
      }
    }'
  ```
- **Response (`202 Accepted`):**
  ```json
  {
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "accepted",
    "schema_version": "1.0",
    "topic": "hermes.transactions.raw",
    "partition": 2,
    "kafka_offset": 10482,
    "correlation_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
  }
  ```
- **DLQ Error Response (`422 Unprocessable Entity`):**
  ```json
  {
    "error_code": "ERR_INVALID_AMOUNT",
    "detail": "amount must be > 0",
    "dlq_routed": true,
    "correlation_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
  }
  ```

### 2.3 Health, Readiness & Metrics
- `GET /health` → `200 OK`
- `GET /readiness` → `200 OK` / `503 Service Unavailable`
- `GET /metrics` → Prometheus exposition format

---

## 3. Fraud Detection Service (`http://localhost:8001`)

### 3.1 Score Single Transaction (`POST /score`)
- **Sample `curl`:**
  ```bash
  curl -X POST http://localhost:8001/score \
    -H "Content-Type: application/json" \
    -H "X-Correlation-ID: 9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d" \
    -d '{
      "event": {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "event_time": "2026-11-12T14:30:00.120Z",
        "customer_id": "c1111111-e29b-41d4-a716-446655440001",
        "merchant_id": "m2222222-e29b-41d4-a716-446655440002",
        "amount": 9450.00,
        "currency": "USD",
        "channel": "online",
        "category": "money_transfer",
        "is_international": true,
        "location": {
          "country": "RU",
          "city": "Moscow",
          "latitude": 55.7558,
          "longitude": 37.6173
        }
      },
      "features": {
        "geo_distance_km": 7510.4,
        "travel_speed_kmph": 4820.5,
        "is_warmup": false,
        "prior_count": 15
      }
    }'
  ```
- **Response (`200 OK`):** Canonical `FraudDecision` v1.0 JSON (Section 10.B) with `fraud_score`, `rule_score`, `ml_score`, `stat_score`, `risk_band`, `decision`, `triggered_rules`, `reasons`, `feature_values`, and `provenance`.

### 3.2 Batch Scoring, Rules & Model Info
- `POST /score/batch` → Scores a list of events in a single micro-batch.
- `GET /rules` → Returns active 10 YAML fraud rules (`rule_version: "rules-v1.0"`).
- `POST /rules/reload` → Hot-reloads `default_rules.yaml`.
- `GET /model/info` → Returns `model_version`, `shadow_model_version`, frozen hybrid weights (`alpha_rule=0.40`, `beta_ml=0.45`, `gamma_stat=0.15`), validation metrics, and live A/B shadow evaluation summary.

---

## 4. Analytics Query Service (`http://localhost:8002`)

All 11 endpoints return `{ "data": ..., "meta": { "source_store": ..., "freshness_sla_seconds": ..., "consistency_model": "bounded_time_range_eventual", "cached": ..., "query_time_ms": ... } }`:

1. `GET /api/v1/analytics/overview?range=24h`
2. `GET /api/v1/analytics/volume?interval=5m&range=24h`
3. `GET /api/v1/analytics/fraud-rate?interval=5m&range=24h`
4. `GET /api/v1/analytics/categories?range=24h`
5. `GET /api/v1/analytics/geo?range=24h`
6. `GET /api/v1/analytics/anomalies?limit=200&range=24h`
7. `GET /api/v1/analytics/transactions?page=1&page_size=20&risk_band=`
8. `GET /api/v1/analytics/alerts?status=active&limit=100` & `PATCH /api/v1/analytics/alerts/{alert_id}`
9. `GET /api/v1/analytics/customers/{customer_id}/profile`
10. `GET /api/v1/analytics/fraud-performance?range=7d`
11. `GET /api/v1/analytics/compliance/aml?range=7d` & `GET /api/v1/analytics/archive/summary`

---

## 5. Notification Service (`http://localhost:8003`)

- **WebSocket Live Feed:** `ws://localhost:8003/ws/alerts` (broadcasts `FRAUD_ALERT` JSON messages deduplicated by `alert_id` and `event_id`).
- `GET /api/v1/alerts/recent?limit=50` → Returns recent buffered alerts and deduplication counters.
- `POST /api/v1/notify/test` → Publishes a test or live `FRAUD_ALERT` across WebSocket, Email, and Webhook channels.

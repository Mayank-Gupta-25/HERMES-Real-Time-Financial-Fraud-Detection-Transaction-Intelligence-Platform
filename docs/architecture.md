# HERMES Platform Architecture & Technical Blueprint

> **System:** HERMES — Real-Time Financial Fraud Detection & Transaction Intelligence Platform  
> **Course:** Data Engineering (CSL 4030), IIT Jodhpur  
> **Architecture Pattern:** Pure Kappa Architecture & Polyglot Persistence  
> **Baseline Date:** November 2026  

---

## 1. Architectural Overview & Kappa Pattern

HERMES is designed as an event-driven, real-time financial fraud detection and analytical intelligence platform. Instead of a dual-codebase Lambda Architecture (which maintains disparate stream and batch layers), HERMES adheres strictly to a **Pure Kappa Architecture**:
- All data ingestion flows continuously through an immutable, ordered, replayable log (**Apache Kafka**).
- Stream processing (**Apache Flink / PyFlink**) serves as the single source of stateful computation for deduplication, customer/merchant enrichment, sliding event-time windows, behavioral feature extraction, and fraud scoring.
- Historical re-computations, model retraining, or recovery replay simply rewind the Kafka consumer offset to re-run the streaming topology over historical logs, completely eliminating stream/batch code bifurcation and training-serving skew.

```
       +-----------------------+     +-----------------------+
       | Transaction Simulator |     | External Ingestion    |
       +-----------+-----------+     +-----------+-----------+
                   |                             |
                   +--------------+--------------+
                                  |
                                  v
                     +------------------------+
                     | API Gateway (FastAPI)  |
                     | JWT · Rate Limit · DLQ |
                     +-----------+------------+
                                 |
         +-----------------------+-----------------------+
         | Valid (Avro binary)                           | Malformed (JSON)
         v                                               v
+-------------------------+                     +------------------+
| hermes.transactions.raw |                     |    hermes.dlq    |
+------------+------------+                     +------------------+
             |
             v
+------------------------------------------------------------------+
|            Apache Flink / Transaction Stream Processor           |
|                                                                  |
| 1. Deterministic Validation & Normalization                      |
| 2. Stateful event_id Deduplication (TTL 86400s)                  |
| 3. Event-Time Watermarks (BoundedOutOfOrderness = 5000ms)        |
| 4. Customer / Merchant Profile Enrichment (Redis LRU)            |
| 5. Static Windows (1m / 5m / 1h) & 16-Feature Extraction         |
| 6. Prior-Only Welford Baselines (No Temporal Leakage)            |
| 7. Async Hybrid Fraud Scoring (Rules + ML + Stat Z-Score)        |
| 8. Checkpoint Manager (Atomic durable snapshots)                 |
+------------+--------------------+--------------------+-----------+
             |                    |                    |
             v                    v                    v
+-----------------------+ +------------------+ +------------------+
| hermes.transactions.  | | hermes.alerts.   | |  hermes.metrics. |
| scored / clean / flag | |      fraud       | |     realtime     |
+------------+----------+ +--------+---------+ +--------+---------+
             |                     |                    |
   +---------+----------+          |                    |
   |                    |          v                    v
   v                    v   +---------------+   +----------------+
+-------------+ +-------------+ | Notification  |   | TimescaleDB    |
| PostgreSQL  | | MinIO S3    | | Service (WS)  |   | Hypertables &  |
| Operational | | Cold Parquet| +-------+-------+   | Continuous Aggs|
+------+------+ +-------------+         |           +--------+-------+
       |                                |                    |
       +----------------+---------------+--------------------+
                        |
                        v
         +------------------------------+
         | Analytics Service (FastAPI)  |
         | Workload-Routed Federation   |
         +--------------+---------------+
                        |
                        v
         +------------------------------+
         | React 18 Dashboard UI        |
         | Real-Time Command Center     |
         +------------------------------+
```

---

## 2. Canonical Data Granules & Avro Schemas

HERMES strictly standardizes event payloads at the ingestion boundary using versioned Avro schemas (`schema_version = "1.0"`).

### 2.1 `TransactionEvent` v1.0 (`infrastructure/kafka/schemas/transaction_event_v1.avsc`)
- **Primary Granule:** Canonical operational event representing a financial transaction.
- **Deduplication Key:** `event_id` (UUIDv4).
- **Partition Key:** `customer_id` (guaranteeing strict per-customer chronological event ordering within Kafka partitions).
- **Timestamp Standard:** UTC ISO-8601 ending in `Z` (`event_time`).
- **Core Attributes:** `amount` (strictly positive, no artificial upper outlier cap), `currency` (ISO 4217 3-letter), `transaction_type`, `channel`, `card_type`, `category`, `location` (`country`, `city`, `latitude`, `longitude`), `device` (optional, fields remain `null` when missing—never imputed with averages).

### 2.2 `FraudDecision` v1.0 (`infrastructure/kafka/schemas/fraud_decision_v1.avsc`)
- **Decision Granule:** Auditable fraud evaluation record emitted for every processed transaction.
- **Fields:** `event_id`, `event_time`, `customer_id`, `merchant_id`, `amount`, `fraud_score` (`[0.0, 1.0]`), `rule_score`, `ml_score`, `stat_score`, `risk_band` (`low`, `medium`, `high`, `critical`), `decision` (`approve`, `flag`, `block`), `triggered_rules` (list of rule codes), `reasons` (human-readable explanations), `model_version`, `rule_version`, `transformation_version`, `feature_time`, `decision_time`, `processing_time_ms`.
- **Feature Snapshot:** `feature_values` map containing the exact 16 feature values used to score the event.
- **Audit Provenance:** `provenance` record tracking `source_topic`, `partition`, `offset`, `ingestion_time`, `fallback_used`, and `storage_destinations`.

### 2.3 `DLQEvent` (`hermes.dlq`)
- Every malformed or schema-invalid event rejected at the API Gateway or Flink validation boundary is wrapped in a `DLQEvent` with `dlq_id`, `original_payload`, `error_code` (`ERR_MALFORMED_PAYLOAD`, `ERR_MISSING_REQUIRED_FIELD`, `ERR_INVALID_UUID`, `ERR_INVALID_TIMESTAMP`, `ERR_INVALID_AMOUNT`, `ERR_INVALID_COORDINATES`, `ERR_INVALID_CATEGORY_OR_ENUM`, `ERR_UNSUPPORTED_SCHEMA_VERSION`), `ingestion_time`, `schema_version`, and `source_topic`.

---

## 3. Stateful Stream Processing & 16 Behavioral Features

Stateful stream processing is executed by `services/transaction-processor/`:

### 3.1 Event-Time Watermarks & Deduplication
- **Watermark Strategy:** Bounded-out-of-orderness watermark delay of `5000 ms` (`watermark = max_event_time - 5s`). Events arriving within the 5s window are ordered deterministically; events arriving older than the watermark are tagged as `is_late_event = True`.
- **Deduplication:** Maintained via keyed state and Redis `SET NX EX 86400`. If an `event_id` has already been seen within 24 hours, the duplicate event is dropped before mutating customer baselines or window counters (`TC-12`), ensuring each transaction is counted **at most once**.

### 3.2 16 Behavioral Features (`F01`..`F16`)
| ID | Feature Name | Description & Mathematical Formula |
|---|---|---|
| `F01` | `txn_count_1m` | Deduplicated transaction count in rolling 1-minute window ($t - 60\text{s} \le t_i \le t$). |
| `F02` | `txn_count_5m` | Deduplicated transaction count in rolling 5-minute window ($t - 300\text{s} \le t_i \le t$). |
| `F03` | `txn_count_1h` | Deduplicated transaction count in rolling 1-hour window ($t - 3600\text{s} \le t_i \le t$). |
| `F04` | `amount_sum_1h` | Total transaction amount sum in rolling 1-hour window. |
| `F05` | `amount_mean_prior` | Customer running mean $\mu_{t-1}$ computed **strictly from prior transactions** ($t_i < t$). |
| `F06` | `amount_std_prior` | Customer running standard deviation $\sigma_{t-1}$ from Welford variance state. |
| `F07` | `amount_z_score` | Statistical z-score $\frac{x_t - \mu_{t-1}}{\max(\sigma_{t-1}, 1.0)}$. Damped to `0.0` during warm-up. |
| `F08` | `time_since_last_sec` | Elapsed seconds $\Delta t = t - t_{n-1}$ since the customer's previous transaction. |
| `F09` | `geo_distance_km` | Great-circle Haversine distance between current coordinates and previous merchant coordinates. |
| `F10` | `travel_speed_kmph` | Apparent travel speed $\frac{\text{geo\_distance\_km}}{\max(\Delta t / 3600.0, 1/3600.0)}$. |
| `F11` | `is_international` | Binary flag ($1$ if transaction country differs from customer home country, else $0$). |
| `F12` | `merchant_category_freq` | Historical fraction of customer spend in the current merchant category. |
| `F13` | `hour_of_day` | UTC hour integer ($0$ to $23$). |
| `F14` | `day_of_week` | UTC day-of-week integer ($0 = \text{Monday}$ to $6 = \text{Sunday}$). |
| `F15` | `is_weekend` | Binary flag ($1$ if Saturday or Sunday, else $0$). |
| `F16` | `device_change_flag` | Binary flag ($1$ if current device type differs from customer's previous device type). |

### 3.3 Temporal Leakage Prevention & Warm-Up Guard
- **Order of Operations:** For every incoming event $e_t$, the processor reads historical Welford statistics ($\mu_{t-1}, \sigma_{t-1}$) from `customer:{customer_id}:baseline` accumulated strictly from transactions where $t_i < t$. Features are computed and emitted **before** the baseline is updated with $x_t$.
- **Warm-Up Guard:** When `prior_count < 5`, `is_warmup = True` is set and `amount_z_score = 0.0` is enforced. New customer accounts are protected against premature false-positive fraud flagging caused by unstable sample variances.

---

## 4. Hybrid Fraud Detection & Risk Scoring Engine

Fraud scoring in `services/fraud-detection/` evaluates three complementary detection signals:

1. **Deterministic Rule Engine (10 Rules):**
   - `VELOCITY_EXCEEDED`: `txn_count_1m > 5` or `txn_count_5m > 15`
   - `AMOUNT_SPIKE`: `not is_warmup` and `amount_z_score > 4.0` and `amount >= 500.0`
   - `GEO_IMPOSSIBILITY`: `travel_speed_kmph > 900.0` and `geo_distance_km > 100.0` (`action: block`)
   - `CARD_TESTING`: `amount <= 5.0` and `micro_txn_count_10m >= 4`
   - `HIGH_RISK_MERCHANT`: `merchant_blacklisted == true` (`block`) or `merchant_is_high_risk and amount >= 300.0`
   - `UNUSUAL_HOUR`: `amount >= 500.0` and `hour_of_day in [1, 2, 3, 4]`
   - `NEW_DEVICE_HIGH_AMOUNT`: `device_change_flag == 1` and `amount >= 1000.0`
   - `RAPID_SUCCESSION`: `time_since_last_sec < 10.0` and `amount >= 200.0`
   - `INTERNATIONAL_HIGH_AMOUNT`: `is_international == 1` and `amount >= 1000.0`
   - `STRUCTURING`: `8000 <= amount < 10000` and `structuring_txn_count_24h >= 2` (`action: block`)

2. **ML Ensemble Scorer:**
   - Chronologically trained Isolation Forest (unsupervised outlier score) combined with XGBoost Gradient Boosted Decision Trees trained with `scale_pos_weight` to address extreme class imbalance ($~2\%$ fraud rate).

3. **Statistical Scorer:**
   - Sigmoid-transformed z-score signal $S_{\text{stat}} = \frac{1}{1 + e^{-(|z| - 2.5)}}$.

4. **Hybrid Calibration:**
   - Score $R = \alpha S_{\text{rule}} + \beta S_{\text{ml}} + \gamma S_{\text{stat}}$ with weights ($\alpha=0.40, \beta=0.45, \gamma=0.15$) calibrated on a chronological validation split to minimize FPR at $\text{Recall} \ge 0.85$.
   - Any triggered rule with `action: block` forces `decision = "block"` and `risk_band = "critical"`.

---

## 5. Polyglot Persistence & Workload-Aligned Indexing

HERMES partitions persistence responsibilities across four specialized storage systems:

```
+-------------------------------------------------------------------------+
|                       Polyglot Persistence Layer                         |
+-------------------+--------------------+----------------+---------------+
|   PostgreSQL 16   |   TimescaleDB 2.14 |    Redis 7     |   MinIO S3    |
|   (Operational)   |   (Time-Series)    |   (Hot State)  | (Cold Parquet)|
+-------------------+--------------------+----------------+---------------+
| transactions      | transaction_       | customer:      | s3://hermes-  |
| fraud_alerts      |   metrics          |   {id}:profile |   archive/    |
| customers         | category_metrics   | customer:      | transactions/ |
| merchants         | geo_metrics        |   {id}:baseline| event_date=   |
|                   | cagg_tx_metrics_5m | velocity:{id}  |   YYYY-MM-DD/ |
|                   | cagg_tx_metrics_1h | dedup:event:{} |               |
+-------------------+--------------------+----------------+---------------+
```

### 5.1 Workload-Aligned Indexes
- `idx_transactions_customer_time`: `(customer_id, event_time DESC)` for customer history drill-down.
- `idx_transactions_merchant_time`: `(merchant_id, event_time DESC)` for merchant investigation.
- `idx_transactions_risk_time`: `(risk_band, event_time DESC)` for risk filtering and anomaly timelines.
- `idx_transactions_category_time`: `(category, event_time DESC)` for category aggregations.
- `idx_transactions_triggered_rules_gin`: GIN index on `triggered_rules` JSONB for rule analytics.
- `idx_fraud_alerts_status_time`: `(status, event_time DESC)` for active alert queues.

### 5.2 TimescaleDB Hypertables & Continuous Aggregates
- Hypertables partitioned into 1-day chunks (`chunk_time_interval => INTERVAL '1 day'`).
- Continuous aggregates `cagg_tx_metrics_5m` and `cagg_tx_metrics_1h` pre-aggregate volume, TPS, total amount, fraud count, and p95 latency.
- Columnar compression policy activates after 7 days; data retention policy drops raw chunks after 90 days.

---

## 6. Fault Tolerance & Durable Checkpoint Recovery

Fault tolerance conforms to `Lab678_21.pdf` Section 5:
- **Checkpointing Mechanism:** Periodic durable snapshots (`checkpoint_interval_ms = 10000`, configurable down to 5000ms) capture Kafka partition `committed_offsets`, deduplication seen sets, watermarks, customer Welford baselines, and window event buffers atomically to durable disk storage and Redis `flink:checkpoint:latest`.
- **Fault-Injection Verification (`TC-27`):** Tested by injecting an abrupt mid-stream worker crash at event 60 of 100. Upon worker restart:
  1. `restore_latest_checkpoint()` loads `chk-000001` with `restore_outcome = "SUCCESS"` in `< 5000 ms`.
  2. The consumer offset rewinds to offset 45 (simulating Kafka replay of 15 already-committed events).
  3. Restored deduplication state drops all 15 replayed events (`duplicate_dropped_count = 15`).
  4. Final customer baselines and window states after event 100 match an uninterrupted reference run bit-for-bit with zero state corruption and zero duplicate records in PostgreSQL.

---

## 7. Parallelism Tuning & Throughput Scaling Knobs

To scale from baseline demonstration up to sustained high-throughput workloads (up to 10,000 TPS):

1. **Kafka Partitioning:**
   - Default: 6 partitions for transaction and alert topics.
   - Scale-Up: Increase partitions to 12 or 24 (`kafka-topics --alter --partitions 12 --topic hermes.transactions.raw`). Kafka keys by `customer_id` ensure per-customer transaction ordering remains intact across workers.
2. **Flink Parallelism:**
   - Task Slots: Set `taskmanager.numberOfTaskSlots: 6` matching topic partitions.
   - Parallelism: Set `parallelism.default: 6` (or scale to `12` with 2 TaskManagers).
3. **Batching & Connection Pools:**
   - Ingestion: API Gateway uses async `confluent_kafka.Producer` with `linger.ms = 5` and `compression.type = lz4`.
   - Scoring API: `httpx.Limits(max_connections = 100, max_keepalive_connections = 20)`.
   - Sinks: `PostgresOperationalSink` flushes batches every 500ms or 200 records using parameterized upserts (`executemany`).
4. **Redis Hot State:**
   - In-process 60-second LRU caches on `customer:{id}:profile` and `merchant:{id}:profile` absorb burst lookup traffic, keeping Redis network round-trips `< 1 ms`.

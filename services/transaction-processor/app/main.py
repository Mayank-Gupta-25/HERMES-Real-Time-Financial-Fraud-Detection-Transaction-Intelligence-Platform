"""
HERMES Transaction Processor Main Pipeline Runner (`app/main.py` — `MOD-02`, `MOD-03`, `MOD-05`, `MOD-08`).

Orchestrates the end-to-end streaming stages:
1. Raw Ingestion & Deterministic Validation / DLQ Routing (`DeduplicationAndCleaningOperator`)
2. Stateful `event_id` Deduplication & 5s Bounded-Out-Of-Orderness Watermarks
3. Customer & Merchant Redis Enrichment (`EnrichmentOperator`)
4. Static `1m`/`5m`/`1h` Event-Time Windows & 16-Feature Extraction with Prior-Only Welford Baseline (`FeatureExtractor`)
5. Async Hybrid Fraud Scoring & Topic Fan-Out (`FraudScoringJob`)
6. Idempotent Polyglot Persistence Sinks (`PostgresOperationalSink`, `TimescaleMetricsSink`, `RedisStateSink`)
7. Durable Checkpoint Snapshotting & Fault Recovery (`CheckpointManager`)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from fastapi import FastAPI, Response

from .config import settings
from .connectors.kafka_sink import KafkaEventSink
from .connectors.kafka_source import KafkaEventSource
from .connectors.redis_lookup import RedisProfileLookup
from .jobs.enrichment_job import EnrichmentStreamWorker
from .jobs.fraud_scoring_job import AsyncFraudScoringClient, FraudScoringJob
from .operators.checkpoint_manager import CheckpointManager
from .operators.feature_extractor import FeatureExtractor
from .sinks.postgres_sink import PostgresOperationalSink
from .sinks.redis_sink import RedisStateSink
from .sinks.timescale_sink import TimescaleMetricsSink

try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore[import-untyped]
    HAS_PROMETHEUS = True
except ImportError:
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore[assignment]
    HAS_PROMETHEUS = False


class TransactionStreamPipeline:
    """
    End-to-end stateful stream processing pipeline combining deduplication, enrichment,
    windowing, 16-feature extraction, fraud scoring, polyglot sinks, and durable checkpointing.
    """

    def __init__(
        self,
        checkpoint_dir: Optional[str | Path] = None,
        redis_lookup: Optional[RedisProfileLookup] = None,
        source: Optional[KafkaEventSource] = None,
        sink: Optional[KafkaEventSink] = None,
        scoring_fn: Optional[Callable[[Dict[str, Any], Dict[str, Any], Dict[str, Any]], Dict[str, Any]]] = None,
        postgres_sink: Optional[PostgresOperationalSink] = None,
        timescale_sink: Optional[TimescaleMetricsSink] = None,
        redis_sink: Optional[RedisStateSink] = None,
    ) -> None:
        self.redis_lookup = redis_lookup or RedisProfileLookup()
        self.source = source or KafkaEventSource(use_in_memory=True)
        self.sink = sink or KafkaEventSink(use_in_memory=True)

        self.enrichment_worker = EnrichmentStreamWorker(
            source=self.source,
            sink=self.sink,
            redis_lookup=self.redis_lookup,
        )
        self.feature_extractor = FeatureExtractor(redis_client=self.redis_lookup.client)
        self.scoring_client = AsyncFraudScoringClient(scoring_fn=scoring_fn)
        self.scoring_job = FraudScoringJob(
            feature_extractor=self.feature_extractor,
            scoring_client=self.scoring_client,
            sink=self.sink,
            redis_lookup=self.redis_lookup,
        )

        self.postgres_sink = postgres_sink or PostgresOperationalSink()
        self.timescale_sink = timescale_sink or TimescaleMetricsSink()
        self.redis_sink = redis_sink or RedisStateSink(redis_client=self.redis_lookup.client)
        self.checkpoint_manager = CheckpointManager(
            checkpoint_dir=checkpoint_dir,
            redis_client=self.redis_lookup.client,
            checkpoint_interval_ms=settings.checkpoint_interval_ms,
        )
        self.committed_offsets: Dict[str, int] = {}

    def process_raw_event(
        self,
        raw_payload: Dict[str, Any] | str | bytes,
        partition: int = 0,
        offset: int = 0,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Process a single raw payload through validation, deduplication, enrichment,
        feature extraction, fraud scoring, and polyglot persistence sinks.
        Returns `(fraud_decision, dlq_event)`.
        """
        enriched, dlq_event = self.enrichment_worker.process_event(raw_payload)
        self.committed_offsets[str(partition)] = int(offset)

        if dlq_event is not None or enriched is None:
            return None, dlq_event

        provenance = {
            "source_topic": self.source.topic,
            "partition": int(partition),
            "offset": int(offset),
            "ingestion_time": enriched.get("event_time"),
        }
        decision_rec, _ = self.scoring_job.process_enriched_event(
            enriched_event=enriched,
            provenance=provenance,
        )

        # Persist to polyglot operational, time-series, and hot state stores
        self.postgres_sink.upsert_decision(decision_rec)
        self.timescale_sink.ingest_scored_decisions([decision_rec], window_size="1m")
        self.redis_sink.persist_decision_state(decision_rec)

        return decision_rec, None

    def process_raw_batch(
        self,
        raw_payloads: Iterable[Dict[str, Any] | str | bytes],
        start_offset: int = 0,
        partition: int = 0,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process a sequence of raw payloads and return `(scored_decisions, dlq_events)`."""
        decisions: List[Dict[str, Any]] = []
        dlq_events: List[Dict[str, Any]] = []
        for idx, payload in enumerate(raw_payloads):
            offset = start_offset + idx
            dec, dlq = self.process_raw_event(payload, partition=partition, offset=offset)
            if dec is not None:
                decisions.append(dec)
            elif dlq is not None:
                dlq_events.append(dlq)
        return decisions, dlq_events

    def take_checkpoint(self, committed_offsets: Optional[Dict[Any, int]] = None) -> Dict[str, Any]:
        offsets_to_save = committed_offsets if committed_offsets is not None else self.committed_offsets
        return self.checkpoint_manager.save_checkpoint(
            committed_offsets=offsets_to_save,
            dedup_operator=self.enrichment_worker.dedup_operator,
            feature_extractor=self.feature_extractor,
            window_aggregator=self.feature_extractor.window_aggregator,
        )

    def restore_from_checkpoint(self) -> Dict[str, Any]:
        telemetry = self.checkpoint_manager.restore_latest_checkpoint(
            dedup_operator=self.enrichment_worker.dedup_operator,
            feature_extractor=self.feature_extractor,
            window_aggregator=self.feature_extractor.window_aggregator,
        )
        if telemetry.get("restore_outcome") == "SUCCESS":
            self.committed_offsets = dict(telemetry.get("committed_offsets", {}))
        return telemetry

    def get_metrics(self) -> Dict[str, Any]:
        metrics = self.enrichment_worker.get_metrics()
        metrics.update(self.scoring_job.get_metrics())
        metrics["postgres_transactions_count"] = len(self.postgres_sink.transactions_by_event_id)
        metrics["postgres_alerts_count"] = len(self.postgres_sink.alerts_by_event_id)
        metrics["last_checkpoint_id"] = self.checkpoint_manager.last_checkpoint_id
        return metrics


app = FastAPI(title=settings.service_name)
pipeline = TransactionStreamPipeline()


@app.get("/")
def read_root() -> Dict[str, str]:
    return {"status": "ok", "service": settings.service_name}


@app.get("/health")
def health_check() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": settings.service_name,
        "metrics": pipeline.get_metrics(),
    }


@app.get("/metrics")
def metrics_endpoint() -> Response:
    if HAS_PROMETHEUS and generate_latest is not None:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
    return Response(content="hermes_flink_up 1\n", media_type="text/plain")

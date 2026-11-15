"""
Flink Enrichment & Deduplication Job (`enrichment_job.py` — MOD-02-FN-01 & MOD-02-FN-03).

Provides:
1. `build_pyflink_enrichment_topology()`: Configures a PyFlink `StreamExecutionEnvironment`
   with `EXACTLY_ONCE` checkpointing (`10,000 ms`) and `BoundedOutOfOrderness` (`5,000 ms`)
   watermarks when `pyflink` is available.
2. `EnrichmentStreamWorker`: Deterministic streaming pipeline connecting `KafkaEventSource` ->
   `DeduplicationAndCleaningOperator` -> `EnrichmentOperator` -> `KafkaEventSink`
   (`hermes.transactions.enriched` + `hermes.dlq` side output).
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..config import settings
from ..connectors.kafka_sink import KafkaEventSink
from ..connectors.kafka_source import KafkaEventSource
from ..connectors.redis_lookup import RedisProfileLookup
from ..operators.dedup_and_clean import DeduplicationAndCleaningOperator
from ..operators.enrichment import EnrichmentOperator

try:
    from pyflink.common import Configuration, Duration, WatermarkStrategy  # type: ignore[import-untyped]
    from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment  # type: ignore[import-untyped]
    HAS_PYFLINK = True
except ImportError:
    HAS_PYFLINK = False


def build_pyflink_enrichment_topology() -> Dict[str, Any]:
    """
    Construct the PyFlink StreamExecutionEnvironment configuration metadata
    for the HERMES enrichment & deduplication topology.
    """
    topology_spec = {
        "job_name": "hermes-enrichment-and-dedup-job",
        "source_topic": settings.topic_transactions_raw,
        "enriched_sink_topic": settings.topic_transactions_enriched,
        "dlq_sink_topic": settings.topic_dlq,
        "checkpoint_interval_ms": settings.checkpoint_interval_ms,
        "checkpoint_mode": "EXACTLY_ONCE",
        "watermark_strategy": "BoundedOutOfOrderness",
        "watermark_max_out_of_orderness_ms": settings.watermark_max_out_of_orderness_ms,
        "pyflink_runtime_available": HAS_PYFLINK,
    }

    if HAS_PYFLINK:
        try:
            env = StreamExecutionEnvironment.get_execution_environment(Configuration())
            env.enable_checkpointing(settings.checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
            _ = WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_millis(settings.watermark_max_out_of_orderness_ms)
            )
            topology_spec["flink_env_initialized"] = True
        except Exception:
            topology_spec["flink_env_initialized"] = False
    else:
        topology_spec["flink_env_initialized"] = False

    return topology_spec


class EnrichmentStreamWorker:
    """
    Executes the `MOD-02` cleaning, `event_id` deduplication, watermark tracking,
    customer/merchant Redis enrichment, and `hermes.transactions.enriched` / `hermes.dlq`
    publishing loop.
    """

    def __init__(
        self,
        source: Optional[KafkaEventSource] = None,
        sink: Optional[KafkaEventSink] = None,
        redis_lookup: Optional[RedisProfileLookup] = None,
        watermark_delay_ms: int = settings.watermark_max_out_of_orderness_ms,
    ) -> None:
        self.redis_lookup = redis_lookup or RedisProfileLookup()
        self.source = source or KafkaEventSource(use_in_memory=True)
        self.sink = sink or KafkaEventSink(use_in_memory=True)
        self.dedup_operator = DeduplicationAndCleaningOperator(
            watermark_delay_ms=watermark_delay_ms,
            redis_lookup=self.redis_lookup,
            source_topic=self.source.topic,
        )
        self.enrichment_operator = EnrichmentOperator(redis_lookup=self.redis_lookup)

    def process_event(
        self, raw_payload: Dict[str, Any] | str | bytes
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Validate, deduplicate, and enrich a single raw payload, routing outputs to `self.sink`.
        Returns `(enriched_event, dlq_event)`.
        """
        cleaned, dlq_event = self.dedup_operator.process_element(raw_payload)
        if dlq_event is not None:
            self.sink.publish_dlq(dlq_event)
            return None, dlq_event

        if cleaned is None:
            # Duplicate dropped
            return None, None

        enriched = self.enrichment_operator.enrich(cleaned)
        self.sink.publish_enriched(enriched)
        return enriched, None

    def process_batch(
        self, payloads: Iterable[Dict[str, Any] | str | bytes]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process an iterable of payloads and return `(enriched_events, dlq_events)`."""
        enriched_list: List[Dict[str, Any]] = []
        dlq_list: List[Dict[str, Any]] = []

        for raw in payloads:
            enriched, dlq = self.process_event(raw)
            if enriched is not None:
                enriched_list.append(enriched)
            elif dlq is not None:
                dlq_list.append(dlq)

        self.sink.flush()
        return enriched_list, dlq_list

    def run_once(self, max_records: int = 100) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Poll one batch from `self.source`, process through dedup + enrichment, and commit offsets."""
        envelopes = self.source.poll_batch(max_records=max_records)
        if not envelopes:
            return [], []

        last_offsets: Dict[int, int] = {}
        payloads: List[Any] = []
        for env in envelopes:
            payloads.append(env["value"])
            last_offsets[int(env["partition"])] = int(env["offset"])

        enriched_list, dlq_list = self.process_batch(payloads)
        self.source.commit(last_offsets=last_offsets)
        return enriched_list, dlq_list

    def get_metrics(self) -> Dict[str, Any]:
        metrics = self.dedup_operator.get_metrics()
        metrics["enriched_count"] = self.enrichment_operator.enriched_count
        metrics["enrichment_hits"] = self.enrichment_operator.cache_or_redis_hits
        metrics["enrichment_misses"] = self.enrichment_operator.profile_misses
        return metrics

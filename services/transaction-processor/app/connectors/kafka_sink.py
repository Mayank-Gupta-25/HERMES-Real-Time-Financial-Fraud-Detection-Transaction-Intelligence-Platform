"""
Kafka Sink Connector for HERMES Stream Processor (MOD-02-FN-01 & MOD-01-FN-08).

Publishes enriched events (`hermes.transactions.enriched`), DLQ events (`hermes.dlq`),
and downstream scored/alert streams with Avro or UTF-8 JSON encoding.
Provides an in-memory sink buffer fallback when `confluent_kafka` is unavailable
or during unit tests.
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Dict, List, Optional

from ..config import settings
from ..schemas.avro_schemas import serialize_avro

try:
    from confluent_kafka import Producer as ConfluentProducer  # type: ignore[import-untyped]
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    ConfluentProducer = None  # type: ignore[assignment]
    HAS_CONFLUENT_KAFKA = False


class KafkaEventSink:
    """
    Produces records to Kafka topics, recording published messages in `published_by_topic`
    when running in in-memory / test mode.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        use_in_memory: Optional[bool] = None,
        use_avro_for_data_topics: bool = False,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.use_avro_for_data_topics = use_avro_for_data_topics
        self.published_by_topic: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._producer: Any = None

        if use_in_memory is None:
            self._use_in_memory = not HAS_CONFLUENT_KAFKA
        else:
            self._use_in_memory = use_in_memory

        if not self._use_in_memory and HAS_CONFLUENT_KAFKA and ConfluentProducer is not None:
            self._producer = ConfluentProducer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "acks": "all",
                    "enable.idempotence": True,
                }
            )

    def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        key: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Publish a single event to `topic` partitioned by `key`."""
        partition_key = str(key or payload.get("customer_id") or payload.get("source_service") or "default")

        if self.use_avro_for_data_topics and schema_name and topic != settings.topic_dlq:
            wire_bytes = serialize_avro(payload, schema_name=schema_name)
        else:
            wire_bytes = json.dumps(payload, default=str).encode("utf-8")

        envelope = {
            "topic": topic,
            "key": partition_key,
            "payload": payload,
            "wire_bytes": wire_bytes,
        }
        self.published_by_topic[topic].append(envelope)

        if self._producer is not None and not self._use_in_memory:
            self._producer.produce(
                topic=topic,
                key=partition_key.encode("utf-8"),
                value=wire_bytes,
            )
            self._producer.poll(0)

        return envelope

    def publish_enriched(self, enriched_event: Dict[str, Any]) -> Dict[str, Any]:
        return self.publish(
            topic=settings.topic_transactions_enriched,
            payload=enriched_event,
            key=enriched_event.get("customer_id"),
        )

    def publish_dlq(self, dlq_event: Dict[str, Any]) -> Dict[str, Any]:
        return self.publish(
            topic=settings.topic_dlq,
            payload=dlq_event,
            key=dlq_event.get("source_service", "transaction-processor"),
        )

    def flush(self, timeout_sec: float = 2.0) -> None:
        if self._producer is not None and not self._use_in_memory:
            try:
                self._producer.flush(timeout_sec)
            except Exception:
                pass

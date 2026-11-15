#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
KAFKA_TOPICS="${KAFKA_TOPICS_BIN:-kafka-topics}"

echo "Waiting for Kafka at ${BOOTSTRAP_SERVER}..."
until "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; do
  sleep 2
done

# Canonical topic, partition, and retention values from the C04 execution spec.
topics=(
  "hermes.transactions.raw 6 604800000"
  "hermes.transactions.enriched 6 604800000"
  "hermes.transactions.scored 6 604800000"
  "hermes.transactions.flagged 6 1209600000"
  "hermes.transactions.clean 6 259200000"
  "hermes.alerts.fraud 6 1209600000"
  "hermes.metrics.realtime 3 259200000"
  "hermes.dlq 3 2592000000"
)

for definition in "${topics[@]}"; do
  read -r topic partitions retention_ms <<<"$definition"
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists --topic "$topic" \
    --partitions "$partitions" --replication-factor 1 \
    --config "retention.ms=${retention_ms}"

  description=$("$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic "$topic")
  if ! grep -Eq "PartitionCount:[[:space:]]*${partitions}([[:space:]]|$)" <<<"$description"; then
    echo "Topic ${topic} does not have the expected ${partitions} partitions" >&2
    exit 1
  fi
  if ! grep -Eq 'ReplicationFactor:[[:space:]]*1([[:space:]]|$)' <<<"$description"; then
    echo "Topic ${topic} does not have replication factor 1" >&2
    exit 1
  fi
  if ! grep -q "retention.ms=${retention_ms}" <<<"$description"; then
    echo "Topic ${topic} does not have retention.ms=${retention_ms}" >&2
    exit 1
  fi
done

echo "Verified Kafka topics:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list | sort

"""Infrastructure contract checks for the canonical HERMES Kafka topics."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import socket
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "docker-compose.yml"
TOPICS_SCRIPT = ROOT / "infrastructure" / "kafka" / "topics-init.sh"
EXPECTED_TOPICS = {
    "hermes.transactions.raw": (6, 604800000),
    "hermes.transactions.enriched": (6, 604800000),
    "hermes.transactions.scored": (6, 604800000),
    "hermes.transactions.flagged": (6, 1209600000),
    "hermes.transactions.clean": (6, 259200000),
    "hermes.alerts.fraud": (6, 1209600000),
    "hermes.metrics.realtime": (3, 259200000),
    "hermes.dlq": (3, 2592000000),
}


def _compose() -> dict:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker Compose CLI is required to parse docker-compose.yml")
    result = subprocess.run(
        [
            docker,
            "compose",
            "--profile",
            "simulator",
            "-f",
            str(COMPOSE_FILE),
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _topic_specs(script: str) -> dict[str, tuple[int, int]]:
    topics: dict[str, tuple[int, int]] = {}
    for line in script.splitlines():
        match = re.fullmatch(r'\s*"(hermes\.[^ ]+) (\d+) (\d+)"\s*', line)
        if match:
            topics[match.group(1)] = (int(match.group(2)), int(match.group(3)))
    return topics


def test_compose_and_topics_config_valid() -> None:
    compose = _compose()
    script = TOPICS_SCRIPT.read_text(encoding="utf-8")

    assert _topic_specs(script) == EXPECTED_TOPICS
    assert "--create --if-not-exists" in script
    assert "--replication-factor 1" in script
    assert "set -euo pipefail" in script

    services = compose["services"]
    assert {
        "kafka",
        "kafka-init",
        "schema-registry",
        "redis",
        "postgres",
        "timescaledb",
        "minio",
        "minio-init",
        "jaeger",
        "prometheus",
        "grafana",
        "api-gateway",
        "fraud-detection",
        "analytics-service",
        "notification-service",
        "transaction-processor",
        "transaction-simulator",
        "dashboard",
    } <= services.keys()
    assert services["kafka"]["healthcheck"]
    assert services["schema-registry"]["healthcheck"]
    assert services["kafka-init"]["depends_on"]["kafka"]["condition"] == "service_healthy"
    assert services["transaction-simulator"]["profiles"] == ["simulator"]

    for service in (
        "redis",
        "postgres",
        "timescaledb",
        "minio",
        "jaeger",
        "prometheus",
        "grafana",
        "api-gateway",
        "fraud-detection",
        "analytics-service",
        "notification-service",
        "transaction-processor",
        "dashboard",
    ):
        assert services[service].get("healthcheck"), f"{service} is missing a healthcheck"


def test_kafka_topics_exist_when_broker_running() -> None:
    if importlib.util.find_spec("confluent_kafka") is None:
        pytest.skip("confluent-kafka is not installed; broker integration requires it")

    from confluent_kafka.admin import AdminClient, ConfigResource

    bootstrap = os.getenv("KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:9092")
    host, separator, port_text = bootstrap.rpartition(":")
    if not separator or not host or not port_text.isdigit():
        pytest.fail(f"Invalid Kafka bootstrap address: {bootstrap!r}")
    host = host.removeprefix("[").removesuffix("]")
    try:
        with socket.create_connection((host, int(port_text)), timeout=1):
            pass
    except Exception as exc:
        pytest.skip(f"Kafka is unavailable at {bootstrap} (unit-only mode): {exc}")

    client = AdminClient({"bootstrap.servers": bootstrap})
    try:
        metadata = client.list_topics(timeout=5)
    except Exception as exc:
        pytest.fail(f"Kafka is reachable at {bootstrap}, but topic metadata could not be read: {exc}")

    missing_topics = set(EXPECTED_TOPICS) - set(metadata.topics)
    assert not missing_topics, f"Kafka topics are missing at {bootstrap}: {sorted(missing_topics)}"

    for topic, (expected_partitions, expected_retention) in EXPECTED_TOPICS.items():
        topic_metadata = metadata.topics[topic]
        assert len(topic_metadata.partitions) == expected_partitions, (
            f"Kafka topic {topic} at {bootstrap} has {len(topic_metadata.partitions)} partitions; "
            f"expected {expected_partitions}"
        )
        replicated_partitions = {
            partition_id: len(partition.replicas)
            for partition_id, partition in topic_metadata.partitions.items()
        }
        assert all(replication_factor == 1 for replication_factor in replicated_partitions.values()), (
            f"Kafka topic {topic} at {bootstrap} must have replication factor 1; "
            f"observed per-partition replica counts {replicated_partitions}"
        )

        # ConfigResource.Type is the supported enum exposed by confluent-kafka;
        # ConfigResourceType is not exported by the installed 2.16.1 package.
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        try:
            config_futures = client.describe_configs([resource], request_timeout=10)
            configs = config_futures[resource].result(timeout=10)
        except Exception as exc:
            pytest.fail(f"Could not read configuration for Kafka topic {topic} at {bootstrap}: {exc}")

        assert "retention.ms" in configs, (
            f"Kafka topic {topic} at {bootstrap} did not return a retention.ms configuration"
        )
        actual_retention = configs["retention.ms"].value
        assert actual_retention == str(expected_retention), (
            f"Kafka topic {topic} at {bootstrap} has retention.ms={actual_retention}; "
            f"expected {expected_retention}"
        )

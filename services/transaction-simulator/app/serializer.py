"""
Canonical TransactionEvent v1.0 Serializer (Avro + JSON fallback) for the
HERMES Transaction Simulator.

Supports:
  - Fast UTF-8 JSON serialization (`format="json"`)
  - Binary Avro serialization (`format="avro"`) against
    `infrastructure/kafka/schemas/transaction_event_v1.avsc`, with a clean
    deterministic Avro-envelope fallback if `fastavro` is not installed in the
    host Python environment.
"""
from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import fastavro  # type: ignore

    HAS_FASTAVRO = True
except ImportError:
    fastavro = None
    HAS_FASTAVRO = False

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_AVSC_PATH = REPO_ROOT / "infrastructure" / "kafka" / "schemas" / "transaction_event_v1.avsc"

EMBEDDED_TRANSACTION_EVENT_SCHEMA_V1: Dict[str, Any] = {
    "type": "record",
    "name": "TransactionEvent",
    "namespace": "io.hermes.events",
    "doc": "Canonical HERMES operational transaction event granule (Lab 6-7-8 Section 4)",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_time", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "merchant_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string", "default": "USD"},
        {"name": "transaction_type", "type": "string", "default": "purchase"},
        {"name": "channel", "type": "string", "default": "online"},
        {"name": "card_type", "type": "string", "default": "credit"},
        {"name": "category", "type": "string"},
        {
            "name": "location",
            "type": {
                "type": "record",
                "name": "LocationRecord",
                "fields": [
                    {"name": "country", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "latitude", "type": "double"},
                    {"name": "longitude", "type": "double"},
                ],
            },
        },
        {
            "name": "device",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "DeviceRecord",
                    "fields": [
                        {"name": "device_id", "type": ["null", "string"], "default": None},
                        {"name": "ip_address", "type": ["null", "string"], "default": None},
                        {"name": "device_type", "type": "string", "default": "mobile"},
                        {"name": "os", "type": ["null", "string"], "default": None},
                    ],
                },
            ],
            "default": None,
        },
        {"name": "is_international", "type": "boolean", "default": False},
        {"name": "status", "type": "string", "default": "pending"},
        {"name": "schema_version", "type": "string", "default": "1.0"},
    ],
}


def load_avro_schema(schema_path: Optional[Path] = None) -> Dict[str, Any]:
    path = schema_path or DEFAULT_AVSC_PATH
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as f:
                raw_schema = json.load(f)
            if HAS_FASTAVRO:
                return fastavro.parse_schema(raw_schema)
            return raw_schema
        except Exception:
            pass
    if HAS_FASTAVRO:
        return fastavro.parse_schema(EMBEDDED_TRANSACTION_EVENT_SCHEMA_V1)
    return EMBEDDED_TRANSACTION_EVENT_SCHEMA_V1


def to_canonical_event(record: Dict[str, Any], strip_labels: bool = True) -> Dict[str, Any]:
    """Normalize a simulator transaction dict to the canonical v1.0 contract."""
    event_id = str(record.get("event_id") or record.get("transaction_id") or "")
    event_time = str(record.get("event_time") or record.get("timestamp") or "")
    category = str(record.get("category") or record.get("merchant_category") or "")
    raw_device = record.get("device") if "device" in record else record.get("device_info")
    device: Optional[Dict[str, Any]] = None
    if isinstance(raw_device, dict):
        device = {
            "device_id": raw_device.get("device_id"),
            "ip_address": raw_device.get("ip_address"),
            "device_type": raw_device.get("device_type") or "mobile",
            "os": raw_device.get("os"),
        }

    loc = record.get("location") or {}
    canonical: Dict[str, Any] = {
        "event_id": event_id,
        "transaction_id": event_id,
        "event_time": event_time,
        "timestamp": event_time,
        "customer_id": str(record["customer_id"]),
        "merchant_id": str(record["merchant_id"]),
        "amount": float(record["amount"]),
        "currency": str(record.get("currency", "USD")),
        "transaction_type": str(record.get("transaction_type", "purchase")),
        "channel": str(record.get("channel", "online")),
        "card_type": str(record.get("card_type", "credit")),
        "category": category,
        "merchant_category": category,
        "location": {
            "country": str(loc.get("country", "US")),
            "city": str(loc.get("city", "Unknown")),
            "latitude": float(loc.get("latitude", 0.0)),
            "longitude": float(loc.get("longitude", 0.0)),
        },
        "device": device,
        "device_info": dict(device) if device is not None else None,
        "is_international": bool(record.get("is_international", False)),
        "status": str(record.get("status", "pending")),
        "schema_version": str(record.get("schema_version", "1.0")),
    }
    if not strip_labels:
        for k, v in record.items():
            if k.startswith("_") or k in ("is_fraud", "fraud_pattern"):
                canonical[k] = v
    return canonical


class TransactionSerializer:
    """Serializes and deserializes TransactionEvent v1.0 payloads in JSON or Avro."""

    AVRO_FALLBACK_MAGIC = b"AVRO1"

    def __init__(self, serialization_format: str = "json", strip_labels: bool = True):
        fmt = (serialization_format or "json").lower()
        if fmt not in ("json", "avro"):
            raise ValueError(f"Unsupported serialization_format: {serialization_format}")
        self.serialization_format = fmt
        self.strip_labels = strip_labels
        self.schema = load_avro_schema()

    def serialize(self, record: Dict[str, Any]) -> bytes:
        canonical = to_canonical_event(record, strip_labels=self.strip_labels)
        if self.serialization_format == "json":
            return json.dumps(canonical).encode("utf-8")

        avro_record = {
            "event_id": canonical["event_id"],
            "event_time": canonical["event_time"],
            "customer_id": canonical["customer_id"],
            "merchant_id": canonical["merchant_id"],
            "amount": canonical["amount"],
            "currency": canonical["currency"],
            "transaction_type": canonical["transaction_type"],
            "channel": canonical["channel"],
            "card_type": canonical["card_type"],
            "category": canonical["category"],
            "location": canonical["location"],
            "device": canonical["device"],
            "is_international": canonical["is_international"],
            "status": canonical["status"],
            "schema_version": canonical["schema_version"],
        }
        if HAS_FASTAVRO:
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, self.schema, avro_record)
            return buf.getvalue()
        return self.AVRO_FALLBACK_MAGIC + json.dumps(avro_record).encode("utf-8")

    def deserialize(self, payload: bytes) -> Dict[str, Any]:
        if payload.startswith(b"{"):
            return json.loads(payload.decode("utf-8"))
        if payload.startswith(self.AVRO_FALLBACK_MAGIC):
            raw = json.loads(payload[len(self.AVRO_FALLBACK_MAGIC) :].decode("utf-8"))
            return to_canonical_event(raw, strip_labels=self.strip_labels)
        if HAS_FASTAVRO:
            buf = io.BytesIO(payload)
            decoded = fastavro.schemaless_reader(buf, self.schema)
            return to_canonical_event(decoded, strip_labels=self.strip_labels)
        return json.loads(payload.decode("utf-8"))


def serialize_transaction(record: Dict[str, Any], fmt: str = "json", strip_labels: bool = True) -> bytes:
    return TransactionSerializer(serialization_format=fmt, strip_labels=strip_labels).serialize(record)


def deserialize_transaction(payload: bytes, fmt: str = "json") -> Dict[str, Any]:
    return TransactionSerializer(serialization_format=fmt).deserialize(payload)

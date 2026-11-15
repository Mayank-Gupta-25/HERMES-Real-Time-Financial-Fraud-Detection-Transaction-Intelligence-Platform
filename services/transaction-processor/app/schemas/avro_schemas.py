"""
Canonical Avro Schema Loader & Serializer/Deserializer (MOD-01-FN-07).

Supports Confluent/fastavro binary serialization with automatic fallback to
a deterministic Avro-envelope wire representation when `fastavro` is not
installed in lightweight local unit-test environments, while also auto-detecting
UTF-8 JSON fallback payloads on deserialization.
"""
from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, Dict

try:
    import fastavro  # type: ignore[import-untyped]
    HAS_FASTAVRO = True
except ImportError:
    fastavro = None  # type: ignore[assignment]
    HAS_FASTAVRO = False


def _resolve_schemas_dir() -> Path:
    # Navigate from services/transaction-processor/app/schemas/ -> repo root
    repo_root = Path(__file__).resolve().parents[4]
    candidate = repo_root / "infrastructure" / "kafka" / "schemas"
    if candidate.exists():
        return candidate
    # Fallback relative path if invoked from different cwd
    return Path("infrastructure/kafka/schemas").resolve()


SCHEMAS_DIR = _resolve_schemas_dir()
TRANSACTION_SCHEMA_PATH = SCHEMAS_DIR / "transaction_event_v1.avsc"
FRAUD_DECISION_SCHEMA_PATH = SCHEMAS_DIR / "fraud_decision_v1.avsc"


def _load_schema(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        raw_schema = json.load(f)
    if HAS_FASTAVRO and fastavro is not None:
        return fastavro.parse_schema(raw_schema)
    return raw_schema


TRANSACTION_EVENT_SCHEMA_V1: Dict[str, Any] = _load_schema(TRANSACTION_SCHEMA_PATH)
FRAUD_DECISION_SCHEMA_V1: Dict[str, Any] = _load_schema(FRAUD_DECISION_SCHEMA_PATH)

_SCHEMA_REGISTRY_MAP: Dict[str, Dict[str, Any]] = {
    "transaction_v1": TRANSACTION_EVENT_SCHEMA_V1,
    "TransactionEvent": TRANSACTION_EVENT_SCHEMA_V1,
    "fraud_decision_v1": FRAUD_DECISION_SCHEMA_V1,
    "FraudDecision": FRAUD_DECISION_SCHEMA_V1,
}

# Magic header for schemaless Avro binary payload identification
_AVRO_MAGIC_HEADER = b"\x00\x00\x00\x00\x01"
_FALLBACK_AVRO_HEADER = b"AVRO1"


def get_schema(schema_name: str = "transaction_v1") -> Dict[str, Any]:
    if schema_name not in _SCHEMA_REGISTRY_MAP:
        raise ValueError(f"Unknown Avro schema name: {schema_name}")
    return _SCHEMA_REGISTRY_MAP[schema_name]


def _validate_against_schema_def(record: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure required schema fields and defaults are populated consistently."""
    if not isinstance(record, dict):
        raise TypeError("Avro record must be a dict")
    normalized: Dict[str, Any] = dict(record)
    for field_spec in schema.get("fields", []):
        fname = field_spec["name"]
        if fname not in normalized:
            if "default" in field_spec:
                default_val = field_spec["default"]
                normalized[fname] = (
                    dict(default_val)
                    if isinstance(default_val, dict)
                    else list(default_val)
                    if isinstance(default_val, list)
                    else default_val
                )
            else:
                raise ValueError(f"Missing required Avro field '{fname}' for schema '{schema.get('name')}'")
    return normalized


def serialize_avro(record: Dict[str, Any], schema_name: str = "transaction_v1") -> bytes:
    """
    Serialize a record dictionary using the canonical v1 Avro schema.
    Uses `fastavro.schemaless_writer` with a 5-byte Schema Registry wire header
    when `fastavro` is installed, or a deterministic binary envelope fallback.
    """
    schema = get_schema(schema_name)
    prepared = _validate_against_schema_def(record, schema)

    if HAS_FASTAVRO and fastavro is not None:
        buf = io.BytesIO()
        buf.write(_AVRO_MAGIC_HEADER)
        fastavro.schemaless_writer(buf, schema, prepared)
        return buf.getvalue()

    encoded_json = json.dumps(prepared, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return _FALLBACK_AVRO_HEADER + encoded_json


def deserialize_avro(payload: bytes | str | Dict[str, Any], schema_name: str = "transaction_v1") -> Dict[str, Any]:
    """
    Deserialize either Avro binary wire bytes or UTF-8 JSON fallback bytes/str
    into a Python dictionary conforming to `schema_name`.
    """
    if isinstance(payload, dict):
        return dict(payload)

    if isinstance(payload, str):
        return json.loads(payload)

    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError(f"Unsupported payload type for deserialize_avro: {type(payload)}")

    raw_bytes = bytes(payload)

    # 1. Check fallback binary header
    if raw_bytes.startswith(_FALLBACK_AVRO_HEADER):
        json_part = raw_bytes[len(_FALLBACK_AVRO_HEADER):].decode("utf-8")
        return json.loads(json_part)

    # 2. Check plain JSON bytes (starts with '{')
    stripped = raw_bytes.lstrip()
    if stripped.startswith(b"{"):
        return json.loads(stripped.decode("utf-8"))

    # 3. Decode Confluent / fastavro schemaless binary payload
    schema = get_schema(schema_name)
    if HAS_FASTAVRO and fastavro is not None:
        offset = 5 if raw_bytes.startswith(_AVRO_MAGIC_HEADER) else 0
        buf = io.BytesIO(raw_bytes[offset:])
        record = fastavro.schemaless_reader(buf, schema)
        return dict(record)

    raise ValueError("Unable to decode payload: neither valid JSON nor supported Avro format")

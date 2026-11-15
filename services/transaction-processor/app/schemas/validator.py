"""
Deterministic Transaction Event Validator, Normalizer & DLQ Builder (MOD-01-FN-07 & MOD-01-FN-08).

Implements the exact cleaning, normalization, and DLQ rules specified in
Lab 6-7-8 Report Section 4 and Blueprint Section 10.A:
1. Maps legacy field aliases (`transaction_id` -> `event_id`, `timestamp` -> `event_time`,
   `merchant_category` -> `category`, `device_info` -> `device`).
2. Enforces `schema_version == "1.0"`.
3. Validates `event_id`, `customer_id`, and `merchant_id` as canonical UUID strings.
4. Normalizes `event_time` to UTC ISO-8601 ending in `Z`.
5. Validates `amount > 0` and finite numeric value with NO upper outlier cap so
   high-value transactions (e.g., $5,000,000.00) are preserved for fraud scoring.
6. Preserves `None` for missing optional `device` fields without mean/dummy imputation.
7. Routes invalid payloads to a structured `DLQEvent` with canonical `error_code`.
"""
from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from .avro_schemas import deserialize_avro

CANONICAL_ERROR_CODES = frozenset(
    {
        "ERR_MALFORMED_PAYLOAD",
        "ERR_MISSING_REQUIRED_FIELD",
        "ERR_INVALID_UUID",
        "ERR_INVALID_TIMESTAMP",
        "ERR_INVALID_AMOUNT",
        "ERR_INVALID_COORDINATES",
        "ERR_INVALID_CATEGORY_OR_ENUM",
        "ERR_UNSUPPORTED_SCHEMA_VERSION",
    }
)

VALID_CHANNELS = frozenset({"online", "pos", "atm", "mobile"})
VALID_TRANSACTION_TYPES = frozenset({"purchase", "withdrawal", "transfer", "refund"})
VALID_CARD_TYPES = frozenset({"credit", "debit", "prepaid"})
VALID_STATUSES = frozenset({"pending", "completed", "declined", "flagged"})

REQUIRED_TOP_LEVEL_FIELDS = (
    "event_id",
    "event_time",
    "customer_id",
    "merchant_id",
    "amount",
    "category",
    "location",
)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_dlq_event(
    original_payload: Any,
    error_code: str,
    error_message: str,
    source_topic: str = "hermes.transactions.raw",
    source_service: str = "transaction-processor",
    schema_version: str = "1.0",
) -> Dict[str, Any]:
    """Construct a canonical DLQEvent conforming to Blueprint Section 10.A."""
    if error_code not in CANONICAL_ERROR_CODES:
        error_code = "ERR_MALFORMED_PAYLOAD"

    if isinstance(original_payload, (bytes, bytearray)):
        try:
            serialized_original = original_payload.decode("utf-8", errors="replace")
        except Exception:
            serialized_original = repr(original_payload)
    elif isinstance(original_payload, str):
        serialized_original = original_payload
    else:
        try:
            serialized_original = json.dumps(original_payload, default=str)
        except Exception:
            serialized_original = repr(original_payload)

    return {
        "dlq_id": f"dlq-{uuid.uuid4()}",
        "original_payload": serialized_original,
        "error_code": error_code,
        "error_message": error_message,
        "ingestion_time": _now_utc_iso(),
        "schema_version": str(schema_version or "1.0"),
        "source_topic": source_topic,
        "source_service": source_service,
    }


def parse_utc_timestamp(value: Any) -> Tuple[Optional[str], Optional[int]]:
    """
    Parse an ISO-8601 timestamp or numeric epoch and return
    `(utc_iso_string_ending_with_Z, epoch_milliseconds)`.
    Returns `(None, None)` if invalid.
    """
    if value is None or isinstance(value, bool):
        return None, None

    dt: Optional[datetime] = None
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value) or value <= 0:
            return None, None
        # Distinguish seconds vs milliseconds
        ts_sec = value / 1000.0 if value > 1e11 else float(value)
        try:
            dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None, None
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None, None
        if raw.endswith("Z") or raw.endswith("z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None, None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    elif isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)

    if dt is None:
        return None, None

    iso_utc = dt.isoformat().replace("+00:00", "Z")
    epoch_ms = int(round(dt.timestamp() * 1000))
    return iso_utc, epoch_ms


def _is_valid_uuid(val: Any) -> bool:
    if not isinstance(val, str) or not val.strip():
        return False
    try:
        parsed = uuid.UUID(val.strip())
        return str(parsed) == val.strip().lower()
    except (ValueError, AttributeError, TypeError):
        return False


def _normalize_aliases(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map legacy Lab 3 / Simulator aliases to canonical Lab 6-7-8 field names."""
    event = dict(raw)

    if "event_id" not in event and "transaction_id" in event:
        event["event_id"] = event["transaction_id"]
    if "event_time" not in event and "timestamp" in event:
        event["event_time"] = event["timestamp"]
    if "category" not in event and "merchant_category" in event:
        event["category"] = event["merchant_category"]
    if "device" not in event and "device_info" in event:
        event["device"] = event["device_info"]

    return event


def validate_and_normalize_event(
    raw_payload: Dict[str, Any] | str | bytes,
    source_topic: str = "hermes.transactions.raw",
    source_service: str = "transaction-processor",
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Validate and deterministically normalize a transaction event.

    Returns:
        `(normalized_event, None)` when valid.
        `(None, dlq_event)` with canonical `error_code` when invalid.
    """
    # 1. Decode payload if str/bytes
    if isinstance(raw_payload, (str, bytes, bytearray)):
        try:
            decoded = deserialize_avro(raw_payload, schema_name="transaction_v1")
        except Exception as exc:
            return None, build_dlq_event(
                original_payload=raw_payload,
                error_code="ERR_MALFORMED_PAYLOAD",
                error_message=f"Failed to decode transaction payload: {exc}",
                source_topic=source_topic,
                source_service=source_service,
            )
    elif isinstance(raw_payload, dict):
        decoded = dict(raw_payload)
    else:
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_MALFORMED_PAYLOAD",
            error_message=f"Expected dict, str, or bytes payload, got {type(raw_payload).__name__}",
            source_topic=source_topic,
            source_service=source_service,
        )

    # 2. Map legacy field aliases
    event = _normalize_aliases(decoded)

    # 3. Validate schema_version
    schema_version = event.get("schema_version", "1.0")
    if schema_version is None or str(schema_version).strip() != "1.0":
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_UNSUPPORTED_SCHEMA_VERSION",
            error_message=f"Unsupported schema_version '{schema_version}'; expected '1.0'",
            source_topic=source_topic,
            source_service=source_service,
            schema_version=str(schema_version),
        )

    # 4. Check required top-level fields exist and are non-null
    for req_field in REQUIRED_TOP_LEVEL_FIELDS:
        if req_field not in event or event[req_field] is None:
            return None, build_dlq_event(
                original_payload=raw_payload,
                error_code="ERR_MISSING_REQUIRED_FIELD",
                error_message=f"Missing required field '{req_field}'",
                source_topic=source_topic,
                source_service=source_service,
            )
        if isinstance(event[req_field], str) and not event[req_field].strip():
            # Empty UUIDs or empty strings in required fields
            if req_field in ("event_id", "customer_id", "merchant_id"):
                return None, build_dlq_event(
                    original_payload=raw_payload,
                    error_code="ERR_MISSING_REQUIRED_FIELD",
                    error_message=f"Required identifier '{req_field}' cannot be empty",
                    source_topic=source_topic,
                    source_service=source_service,
                )
            if req_field == "category":
                return None, build_dlq_event(
                    original_payload=raw_payload,
                    error_code="ERR_MISSING_REQUIRED_FIELD",
                    error_message="Required field 'category' cannot be empty",
                    source_topic=source_topic,
                    source_service=source_service,
                )

    # 5. Validate UUID identifiers (event_id, customer_id, merchant_id)
    for uuid_field in ("event_id", "customer_id", "merchant_id"):
        raw_uuid = event.get(uuid_field)
        if not _is_valid_uuid(raw_uuid):
            return None, build_dlq_event(
                original_payload=raw_payload,
                error_code="ERR_INVALID_UUID",
                error_message=f"Field '{uuid_field}' must be a valid UUID string, got '{raw_uuid}'",
                source_topic=source_topic,
                source_service=source_service,
            )

    # 6. Validate and normalize event_time to UTC ISO-8601 ending in 'Z'
    normalized_time, _ = parse_utc_timestamp(event.get("event_time"))
    if normalized_time is None:
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_TIMESTAMP",
            error_message=f"Invalid event_time '{event.get('event_time')}'; must be valid ISO-8601 timestamp",
            source_topic=source_topic,
            source_service=source_service,
        )

    # 7. Validate amount (> 0, finite float, NO upper outlier cap!)
    raw_amount = event.get("amount")
    if isinstance(raw_amount, bool) or not isinstance(raw_amount, (int, float)):
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_AMOUNT",
            error_message=f"amount must be a numeric int/float, got {type(raw_amount).__name__}",
            source_topic=source_topic,
            source_service=source_service,
        )
    amount_val = float(raw_amount)
    if math.isnan(amount_val) or math.isinf(amount_val) or amount_val <= 0.0:
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_AMOUNT",
            error_message=f"amount must be finite and strictly positive (> 0), got {raw_amount}",
            source_topic=source_topic,
            source_service=source_service,
        )

    # 8. Normalize and validate currency, category, channel, transaction_type, card_type, status
    raw_currency = event.get("currency", "USD")
    if not isinstance(raw_currency, str) or len(raw_currency.strip()) != 3 or not raw_currency.strip().isalpha():
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_CATEGORY_OR_ENUM",
            error_message=f"currency must be a 3-letter ISO code, got '{raw_currency}'",
            source_topic=source_topic,
            source_service=source_service,
        )
    currency = raw_currency.strip().upper()

    raw_category = event.get("category")
    if not isinstance(raw_category, str) or not raw_category.strip():
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_CATEGORY_OR_ENUM",
            error_message=f"category must be a non-empty string, got '{raw_category}'",
            source_topic=source_topic,
            source_service=source_service,
        )
    category = raw_category.strip().lower()

    transaction_type = str(event.get("transaction_type") or "purchase").strip().lower()
    if transaction_type not in VALID_TRANSACTION_TYPES:
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_CATEGORY_OR_ENUM",
            error_message=f"Invalid transaction_type '{transaction_type}'",
            source_topic=source_topic,
            source_service=source_service,
        )

    channel = str(event.get("channel") or "online").strip().lower()
    if channel not in VALID_CHANNELS:
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_CATEGORY_OR_ENUM",
            error_message=f"Invalid channel '{channel}'",
            source_topic=source_topic,
            source_service=source_service,
        )

    card_type = str(event.get("card_type") or "credit").strip().lower()
    if card_type not in VALID_CARD_TYPES:
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_CATEGORY_OR_ENUM",
            error_message=f"Invalid card_type '{card_type}'",
            source_topic=source_topic,
            source_service=source_service,
        )

    status = str(event.get("status") or "pending").strip().lower()
    if status not in VALID_STATUSES:
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_CATEGORY_OR_ENUM",
            error_message=f"Invalid status '{status}'",
            source_topic=source_topic,
            source_service=source_service,
        )

    # 9. Validate location & coordinates
    loc = event.get("location")
    if not isinstance(loc, dict):
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_MISSING_REQUIRED_FIELD",
            error_message="location must be a JSON object with country, city, latitude, longitude",
            source_topic=source_topic,
            source_service=source_service,
        )

    for loc_field in ("country", "city", "latitude", "longitude"):
        if loc_field not in loc or loc[loc_field] is None:
            return None, build_dlq_event(
                original_payload=raw_payload,
                error_code="ERR_MISSING_REQUIRED_FIELD",
                error_message=f"Missing required location field 'location.{loc_field}'",
                source_topic=source_topic,
                source_service=source_service,
            )

    raw_country = loc.get("country")
    if not isinstance(raw_country, str) or len(raw_country.strip()) != 2 or not raw_country.strip().isalpha():
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_COORDINATES",
            error_message=f"location.country must be a 2-letter ISO country code, got '{raw_country}'",
            source_topic=source_topic,
            source_service=source_service,
        )

    raw_lat = loc.get("latitude")
    raw_lon = loc.get("longitude")
    if (
        isinstance(raw_lat, bool)
        or isinstance(raw_lon, bool)
        or not isinstance(raw_lat, (int, float))
        or not isinstance(raw_lon, (int, float))
    ):
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_COORDINATES",
            error_message="location.latitude and location.longitude must be numeric",
            source_topic=source_topic,
            source_service=source_service,
        )

    lat = float(raw_lat)
    lon = float(raw_lon)
    if (
        math.isnan(lat)
        or math.isinf(lat)
        or math.isnan(lon)
        or math.isinf(lon)
        or not (-90.0 <= lat <= 90.0)
        or not (-180.0 <= lon <= 180.0)
    ):
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_INVALID_COORDINATES",
            error_message=f"Invalid coordinates latitude={lat}, longitude={lon}; expected lat in [-90, 90], lon in [-180, 180]",
            source_topic=source_topic,
            source_service=source_service,
        )

    normalized_location = {
        "country": raw_country.strip().upper(),
        "city": str(loc["city"]).strip(),
        "latitude": lat,
        "longitude": lon,
    }

    # 10. Normalize optional device field (preserve None for missing optional fields, never impute!)
    raw_device = event.get("device")
    normalized_device: Optional[Dict[str, Any]]
    if raw_device is None:
        normalized_device = None
    elif isinstance(raw_device, dict):
        dev_id = raw_device.get("device_id")
        ip_addr = raw_device.get("ip_address")
        dev_type = raw_device.get("device_type")
        os_name = raw_device.get("os")
        normalized_device = {
            "device_id": str(dev_id).strip() if dev_id is not None and str(dev_id).strip() != "" else None,
            "ip_address": str(ip_addr).strip() if ip_addr is not None and str(ip_addr).strip() != "" else None,
            "device_type": str(dev_type).strip().lower() if dev_type is not None and str(dev_type).strip() != "" else "mobile",
            "os": str(os_name).strip() if os_name is not None and str(os_name).strip() != "" else None,
        }
    else:
        return None, build_dlq_event(
            original_payload=raw_payload,
            error_code="ERR_MALFORMED_PAYLOAD",
            error_message=f"device must be null or an object, got {type(raw_device).__name__}",
            source_topic=source_topic,
            source_service=source_service,
        )

    normalized_event: Dict[str, Any] = {
        "event_id": str(event["event_id"]).strip().lower(),
        "event_time": normalized_time,
        "customer_id": str(event["customer_id"]).strip().lower(),
        "merchant_id": str(event["merchant_id"]).strip().lower(),
        "amount": amount_val,
        "currency": currency,
        "transaction_type": transaction_type,
        "channel": channel,
        "card_type": card_type,
        "category": category,
        "location": normalized_location,
        "device": normalized_device,
        "is_international": bool(event.get("is_international", False)),
        "status": status,
        "schema_version": "1.0",
    }

    return normalized_event, None

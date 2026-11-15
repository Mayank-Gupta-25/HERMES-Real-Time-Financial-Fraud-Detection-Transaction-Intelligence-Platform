from __future__ import annotations

import math
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator


class LocationModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    latitude: float = Field(..., ge=-90.0, le=90.0, description="Latitude in [-90.0, 90.0]")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="Longitude in [-180.0, 180.0]")
    country: str = Field(..., min_length=2, max_length=2, description="ISO 3166-1 alpha-2 country code")
    city: str = Field(..., min_length=1, description="City name")

    @field_validator("country")
    @classmethod
    def normalize_country(cls, value: str) -> str:
        cleaned = value.strip().upper()
        if len(cleaned) != 2 or not cleaned.isalpha():
            raise ValueError("country must be a 2-letter ISO 3166-1 alpha-2 code")
        return cleaned

    @field_validator("city")
    @classmethod
    def validate_city(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("city must not be empty")
        return cleaned


class DeviceModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    device_id: Optional[str] = None
    device_type: Optional[str] = None
    os: Optional[str] = None
    browser: Optional[str] = None
    ip_address: Optional[str] = None


class TransactionCreateRequest(BaseModel):
    """Canonical TransactionEvent v1.0 request schema with backward-compatible field aliases.

    Follows Lab678_21.pdf Section 4:
    - Accepts canonical fields (`event_id`, `event_time`, `category`, `device`) as well as
      legacy simulator/client aliases (`transaction_id`, `timestamp`, `merchant_category`, `device_info`).
    - Normalizes `event_time` deterministically to UTC ISO-8601 (`Z`).
    - Enforces `amount > 0` with NO artificial upper cap so high-value outlier transactions are preserved.
    - Keeps missing optional `device` fields explicitly `None` (no mean/dummy imputation).
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    event_id: str = Field(
        ...,
        validation_alias=AliasChoices("event_id", "transaction_id"),
        description="UUIDv4 unique event identifier (deduplication key)",
    )
    event_time: str = Field(
        ...,
        validation_alias=AliasChoices("event_time", "timestamp"),
        description="UTC ISO-8601 event timestamp ending in Z",
    )
    customer_id: str = Field(..., description="UUIDv4 customer identifier (Kafka partition key)")
    merchant_id: str = Field(..., description="UUIDv4 merchant identifier")
    amount: float = Field(..., gt=0.0, description="Strictly positive transaction amount (> 0.0)")
    currency: str = Field(default="USD", min_length=3, max_length=3, description="ISO 4217 3-letter currency code")
    transaction_type: str = Field(default="purchase", description="purchase | withdrawal | transfer | refund")
    channel: str = Field(default="online", description="online | pos | atm | mobile")
    card_type: str = Field(default="credit", description="credit | debit | prepaid")
    category: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("category", "merchant_category"),
        description="Canonical merchant category",
    )
    location: LocationModel
    device: Optional[DeviceModel] = Field(
        default=None,
        validation_alias=AliasChoices("device", "device_info"),
        description="Optional client device metadata (never imputed when absent)",
    )
    is_international: bool = Field(default=False)
    status: str = Field(default="pending")
    schema_version: str = Field(default="1.0")

    @field_validator("event_id", "customer_id", "merchant_id")
    @classmethod
    def validate_uuid_fields(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("UUID field must be a non-empty string")
        try:
            parsed = uuid.UUID(value.strip())
        except (ValueError, AttributeError, TypeError) as exc:
            raise ValueError(f"Invalid UUID format: {value!r}") from exc
        return str(parsed)

    @field_validator("event_time", mode="before")
    @classmethod
    def validate_and_normalize_event_time(cls, value: Any) -> str:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
        elif isinstance(value, str):
            raw = value.strip()
            if not raw:
                raise ValueError("event_time must not be empty")
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(raw)
            except ValueError as exc:
                raise ValueError(f"Invalid ISO-8601 timestamp: {value!r}") from exc
        else:
            raise ValueError(f"Unsupported timestamp type: {type(value).__name__}")

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt.isoformat().replace("+00:00", "Z")

    @field_validator("amount")
    @classmethod
    def validate_finite_positive_amount(cls, value: float) -> float:
        if math.isnan(value) or math.isinf(value) or value <= 0.0:
            raise ValueError("amount must be a finite number strictly greater than 0")
        return float(value)

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        cleaned = value.strip().upper()
        if len(cleaned) != 3 or not cleaned.isalpha():
            raise ValueError("currency must be a 3-letter ISO 4217 code")
        return cleaned

    @field_validator("category")
    @classmethod
    def normalize_category(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if not cleaned:
            raise ValueError("category must not be empty")
        return cleaned

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        cleaned = value.strip()
        if cleaned != "1.0":
            raise ValueError(f"Unsupported schema_version: {cleaned!r}; expected '1.0'")
        return cleaned

    @property
    def transaction_id(self) -> str:
        return self.event_id

    @property
    def timestamp(self) -> str:
        return self.event_time

    @property
    def merchant_category(self) -> str:
        return self.category

    @property
    def device_info(self) -> Optional[DeviceModel]:
        return self.device

    def to_canonical_dict(self) -> dict[str, Any]:
        """Return the exact Section 10.A TransactionEvent v1.0 dictionary."""
        device_dict: Optional[dict[str, Any]] = None
        if self.device is not None:
            device_dict = {
                "device_id": self.device.device_id,
                "ip_address": self.device.ip_address,
                "device_type": self.device.device_type or "mobile",
                "os": self.device.os,
                "browser": self.device.browser,
            }

        return {
            "event_id": self.event_id,
            "event_time": self.event_time,
            "customer_id": self.customer_id,
            "merchant_id": self.merchant_id,
            "amount": self.amount,
            "currency": self.currency,
            "transaction_type": self.transaction_type,
            "channel": self.channel,
            "card_type": self.card_type,
            "category": self.category,
            "location": {
                "country": self.location.country,
                "city": self.location.city,
                "latitude": self.location.latitude,
                "longitude": self.location.longitude,
            },
            "device": device_dict,
            "is_international": self.is_international,
            "status": self.status,
            "schema_version": self.schema_version,
        }

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class TransactionAcceptedResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(..., description="Canonical UUIDv4 event identifier")
    transaction_id: str = Field(..., description="Backward-compatible alias for event_id")
    status: str = Field(default="accepted")
    schema_version: str = Field(default="1.0")
    topic: str = Field(default="hermes.transactions.raw")
    partition: int = Field(default=0)
    kafka_offset: int = Field(default=0)
    correlation_id: str = Field(..., description="Propagated X-Correlation-ID trace identifier")


class DLQErrorResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    error_code: str = Field(..., description="Canonical DLQ reason code (e.g. ERR_INVALID_AMOUNT)")
    detail: str = Field(..., description="Human-readable validation failure description")
    dlq_routed: bool = Field(default=True, description="Whether the rejected payload was routed to hermes.dlq")
    correlation_id: Optional[str] = Field(default=None, description="Propagated X-Correlation-ID trace identifier")

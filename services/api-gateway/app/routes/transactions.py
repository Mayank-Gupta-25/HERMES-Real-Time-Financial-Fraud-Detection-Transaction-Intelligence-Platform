from __future__ import annotations

import uuid
from typing import Optional

from fastapi import APIRouter, Request, status
from pydantic import BaseModel, Field

from ..config import settings
from ..middleware.auth import create_access_token
from ..models.response import TransactionAcceptedResponse
from ..models.transaction import TransactionCreateRequest
from ..services.kafka_producer import GatewayKafkaProducer, get_kafka_producer
from .health import TRANSACTIONS_INGESTED_TOTAL

router = APIRouter(prefix="/api/v1", tags=["transactions"])


class TokenRequest(BaseModel):
    client_id: str = Field(default="demo-client", min_length=1)
    role: str = Field(default="ingest_client")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


def _resolve_producer(request: Request) -> GatewayKafkaProducer:
    producer: Optional[GatewayKafkaProducer] = getattr(request.app.state, "kafka_producer", None)
    if producer is not None:
        return producer
    return get_kafka_producer()


@router.post(
    "/transactions",
    response_model=TransactionAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_transaction(
    payload: TransactionCreateRequest,
    request: Request,
) -> TransactionAcceptedResponse:
    """Validate and publish a TransactionEvent v1.0 to `hermes.transactions.raw` keyed by `customer_id`."""
    correlation_id = (
        getattr(request.state, "correlation_id", None)
        or request.headers.get("X-Correlation-ID")
        or str(uuid.uuid4())
    )
    canonical_event = payload.to_canonical_dict()
    producer = _resolve_producer(request)

    meta = producer.publish_transaction(canonical_event, correlation_id=correlation_id)
    TRANSACTIONS_INGESTED_TOTAL.labels(
        source="api",
        channel=canonical_event.get("channel", "online"),
        status="valid",
    ).inc()

    return TransactionAcceptedResponse(
        event_id=payload.event_id,
        transaction_id=payload.transaction_id,
        status="accepted",
        schema_version=payload.schema_version,
        topic=meta["topic"],
        partition=int(meta["partition"]),
        kafka_offset=int(meta["offset"]),
        correlation_id=correlation_id,
    )


@router.post("/auth/token", response_model=TokenResponse)
async def issue_dev_token(body: Optional[TokenRequest] = None) -> TokenResponse:
    """Helper endpoint to issue signed JWT tokens for demo scripts and Locust load tests."""
    req = body or TokenRequest()
    token = create_access_token(subject=req.client_id, role=req.role)
    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=settings.jwt_expire_minutes * 60,
    )

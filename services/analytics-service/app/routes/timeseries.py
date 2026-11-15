from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Query

from ..config import settings
from ..db.timescale import timescale_db
from ..services.query_cache import query_cache

router = APIRouter(tags=["Timeseries"])


@router.get("/volume")
async def get_volume_timeseries(
    interval: str = Query(default="5m", description="Bucket interval (1m, 5m, 15m, 1h)"),
    range: str = Query(default="24h", description="Bounded time range (1h, 24h, 7d)"),
) -> dict[str, Any]:
    """
    GET /api/v1/analytics/volume
    Queries TimescaleDB `transaction_metrics` / continuous aggregates using `time_bucket`.
    """
    return await query_cache.execute_with_envelope(
        endpoint="volume",
        params={"interval": interval, "range": range},
        fetcher=lambda: timescale_db.get_volume_series(interval=interval, range_str=range),
        source_store="timescaledb",
        freshness_sla_seconds=5,
        ttl_seconds=settings.live_cache_ttl_seconds,
        range_str=range,
    )


@router.get("/fraud-rate")
async def get_fraud_rate_timeseries(
    interval: str = Query(default="5m", description="Bucket interval (1m, 5m, 15m, 1h)"),
    range: str = Query(default="24h", description="Bounded time range (1h, 24h, 7d)"),
) -> dict[str, Any]:
    """
    GET /api/v1/analytics/fraud-rate
    Returns time-series fraud rate + breakdown by `risk_band`.
    """
    return await query_cache.execute_with_envelope(
        endpoint="fraud-rate",
        params={"interval": interval, "range": range},
        fetcher=lambda: timescale_db.get_fraud_rate_series(interval=interval, range_str=range),
        source_store="timescaledb",
        freshness_sla_seconds=5,
        ttl_seconds=settings.live_cache_ttl_seconds,
        range_str=range,
    )

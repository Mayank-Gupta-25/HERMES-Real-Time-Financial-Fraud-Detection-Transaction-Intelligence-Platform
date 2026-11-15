from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Query

from ..config import settings
from ..db.postgres import postgres_db
from ..services.query_cache import query_cache

router = APIRouter(tags=["Anomalies"])


@router.get("/anomalies")
async def get_anomalies_timeline(
    limit: int = Query(default=200, ge=1, le=1000, description="Max anomaly points to return"),
    range: str = Query(default="24h", description="Bounded time window (e.g. 24h, 7d)"),
) -> dict[str, Any]:
    """
    GET /api/v1/analytics/anomalies
    Returns high-risk/flagged transactions (`fraud_score >= 0.40`) ordered by `event_time DESC`
    for the anomaly scatter timeline.
    """
    return await query_cache.execute_with_envelope(
        endpoint="anomalies",
        params={"limit": limit, "range": range},
        fetcher=lambda: postgres_db.get_anomalies(limit=limit, range_str=range),
        source_store="postgresql",
        freshness_sla_seconds=5,
        ttl_seconds=settings.live_cache_ttl_seconds,
        range_str=range,
    )

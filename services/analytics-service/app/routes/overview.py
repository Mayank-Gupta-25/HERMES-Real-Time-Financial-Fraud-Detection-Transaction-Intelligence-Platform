from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Query

from ..config import settings
from ..db.postgres import postgres_db
from ..services.query_cache import query_cache

router = APIRouter(tags=["Overview"])


@router.get("/overview")
async def get_overview(
    range: str = Query(default="24h", description="Bounded time window (e.g. 1h, 24h, 7d)"),
) -> dict[str, Any]:
    """
    GET /api/v1/analytics/overview
    Returns KPI summary (`current_tps`, `total_transactions`, `total_volume_usd`, `fraud_rate`,
    `flagged_count`, `blocked_count`, `avg_amount`, `p95_latency_ms`, `sparklines`) + federation `meta`.
    """
    return await query_cache.execute_with_envelope(
        endpoint="overview",
        params={"range": range},
        fetcher=lambda: postgres_db.get_overview_stats(range_str=range),
        source_store="postgresql+timescaledb+redis",
        freshness_sla_seconds=5,
        ttl_seconds=settings.live_cache_ttl_seconds,
        range_str=range,
    )

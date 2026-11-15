from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Query

from ..config import settings
from ..db.postgres import postgres_db
from ..services.query_cache import query_cache

router = APIRouter(tags=["Categories"])


@router.get("/categories")
async def get_category_breakdown(
    range: str = Query(default="24h", description="Bounded time window (e.g. 24h, 7d)"),
) -> dict[str, Any]:
    """
    GET /api/v1/analytics/categories
    Aggregates transaction volume and fraud rate per merchant category.
    """
    return await query_cache.execute_with_envelope(
        endpoint="categories",
        params={"range": range},
        fetcher=lambda: postgres_db.get_categories(range_str=range),
        source_store="postgresql",
        freshness_sla_seconds=15,
        ttl_seconds=settings.historical_cache_ttl_seconds,
        range_str=range,
    )

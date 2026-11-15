from __future__ import annotations

from typing import Any
from fastapi import APIRouter, HTTPException

from ..config import settings
from ..db.postgres import postgres_db
from ..db.redis import redis_client
from ..services.query_cache import query_cache

router = APIRouter(tags=["Customer Intelligence"])


@router.get("/customers/{customer_id}/profile")
async def get_customer_360_profile(customer_id: str) -> dict[str, Any]:
    """
    GET /api/v1/analytics/customers/{customer_id}/profile
    Combines PostgreSQL customer metadata + recent transactions with Redis hot Welford baseline
    (`customer:{customer_id}:baseline` and `velocity:{customer_id}`) to return a 360-degree profile.
    """

    async def _fetch_profile() -> dict[str, Any]:
        pg_data = await postgres_db.get_customer_profile_data(customer_id)
        if pg_data is None:
            raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found")

        cust = pg_data["customer"]
        cid = cust["customer_id"]
        hot_baseline = await redis_client.get_customer_hot_baseline(cid)

        return {
            "customer_id": cid,
            "full_name": cust["full_name"],
            "email": cust["email"],
            "home_country": cust["home_country"],
            "home_city": cust["home_city"],
            "home_latitude": cust["home_latitude"],
            "home_longitude": cust["home_longitude"],
            "risk_tier": cust["risk_tier"],
            "card_type": cust["card_type"],
            "account_created_at": cust["account_created_at"],
            "prior_count": hot_baseline["prior_count"],
            "is_warmup": hot_baseline["is_warmup"],
            "amount_mean_prior": hot_baseline["amount_mean_prior"],
            "amount_std_prior": hot_baseline["amount_std_prior"],
            "txn_count_1m": hot_baseline["txn_count_1m"],
            "txn_count_5m": hot_baseline["txn_count_5m"],
            "txn_count_1h": hot_baseline["txn_count_1h"],
            "velocity": {
                "txn_count_1m": hot_baseline["txn_count_1m"],
                "txn_count_5m": hot_baseline["txn_count_5m"],
                "txn_count_1h": hot_baseline["txn_count_1h"],
            },
            "baseline": hot_baseline,
            "category_breakdown": pg_data["category_breakdown"],
            "recent_transactions": pg_data["recent_transactions"],
            "active_alerts": pg_data["active_alerts"],
        }

    return await query_cache.execute_with_envelope(
        endpoint=f"customer_profile:{customer_id}",
        params={"customer_id": customer_id},
        fetcher=_fetch_profile,
        source_store="postgresql+redis",
        freshness_sla_seconds=2,
        ttl_seconds=settings.live_cache_ttl_seconds,
        range_str="7d",
    )

from __future__ import annotations

from typing import Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..config import settings
from ..db.postgres import postgres_db
from ..services.query_cache import query_cache

router = APIRouter(tags=["Transactions & Alerts"])


class AlertUpdateRequest(BaseModel):
    status: str = Field(..., description="New alert status: active | acknowledged | dismissed")
    analyst_notes: str | None = Field(default=None, description="Optional analyst review notes")


@router.get("/transactions")
async def list_transactions(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    sort_by: str = Query(default="event_time"),
    sort_dir: str = Query(default="desc"),
    risk_band: str | None = Query(default=None),
    decision: str | None = Query(default=None),
    customer_id: str | None = Query(default=None),
    category: str | None = Query(default=None),
) -> dict[str, Any]:
    """
    GET /api/v1/analytics/transactions
    Paginated operational transaction explorer using workload-aligned B-tree indexes.
    """
    params = {
        "page": page,
        "page_size": page_size,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "risk_band": risk_band or "",
        "decision": decision or "",
        "customer_id": customer_id or "",
        "category": category or "",
    }
    return await query_cache.execute_with_envelope(
        endpoint="transactions",
        params=params,
        fetcher=lambda: postgres_db.get_transactions(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            risk_band=risk_band or None,
            decision=decision or None,
            customer_id=customer_id or None,
            category=category or None,
        ),
        source_store="postgresql",
        freshness_sla_seconds=2,
        ttl_seconds=settings.live_cache_ttl_seconds,
        range_str="24h",
    )


@router.get("/alerts")
async def list_alerts(
    status: str | None = Query(default="active", description="active | acknowledged | dismissed | all"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    """
    GET /api/v1/analytics/alerts
    Returns fraud alerts filtered by status using `idx_fraud_alerts_status_time`.
    """
    return await query_cache.execute_with_envelope(
        endpoint="alerts",
        params={"status": status or "all", "limit": limit},
        fetcher=lambda: postgres_db.get_alerts(status=status, limit=limit),
        source_store="postgresql",
        freshness_sla_seconds=2,
        ttl_seconds=settings.live_cache_ttl_seconds,
        range_str="24h",
    )


@router.patch("/alerts/{alert_id}")
async def patch_alert_status(
    alert_id: str,
    payload: AlertUpdateRequest,
) -> dict[str, Any]:
    """
    PATCH /api/v1/analytics/alerts/{alert_id}
    Updates alert status (`active`, `acknowledged`, `dismissed`) and optional `analyst_notes`.
    """
    allowed_statuses = {"active", "acknowledged", "dismissed"}
    target_status = payload.status.strip().lower()
    if target_status not in allowed_statuses:
        raise HTTPException(
            status_code=422,
            detail=f"status must be one of {sorted(allowed_statuses)}, got '{payload.status}'",
        )

    updated = await postgres_db.update_alert(
        alert_id=alert_id,
        status=target_status,
        analyst_notes=payload.analyst_notes,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail=f"Alert '{alert_id}' not found")

    # Invalidate cached alert/overview responses after state transition
    query_cache.invalidate_all()

    return await query_cache.execute_with_envelope(
        endpoint=f"patch_alert:{alert_id}",
        params={"alert_id": alert_id, "status": target_status},
        fetcher=lambda: _async_identity(updated),
        source_store="postgresql",
        freshness_sla_seconds=0,
        bypass_cache=True,
    )


async def _async_identity(val: Any) -> Any:
    return val

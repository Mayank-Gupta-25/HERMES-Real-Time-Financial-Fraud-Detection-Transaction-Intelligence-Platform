from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import time
from typing import Any
import uuid

from fastapi import WebSocket

from ..config import settings


def normalize_fraud_alert(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize incoming alert dictionary to the exact Section 10.F.1 WebSocket
    `FRAUD_ALERT` contract.
    """
    event_id = str(
        raw.get("event_id")
        or raw.get("transaction_id")
        or uuid.uuid4()
    )
    alert_id = str(raw.get("alert_id") or f"alert-{event_id}")
    event_time = str(
        raw.get("event_time")
        or raw.get("timestamp")
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    risk_band = str(raw.get("risk_band") or raw.get("risk_level") or "high").lower()
    decision = str(
        raw.get("decision") or ("block" if risk_band == "critical" else "flag")
    ).lower()
    triggered_rules = list(raw.get("triggered_rules") or [])

    return {
        "type": "FRAUD_ALERT",
        "alert_id": alert_id,
        "event_id": event_id,
        "transaction_id": event_id,
        "event_time": event_time,
        "customer_id": str(raw.get("customer_id") or "00000000-0000-0000-0000-000000000000"),
        "merchant_id": str(raw.get("merchant_id") or "00000000-0000-0000-0000-000000000000"),
        "amount": float(raw.get("amount", 0.0) or 0.0),
        "currency": str(raw.get("currency") or "USD").upper(),
        "category": str(raw.get("category") or raw.get("merchant_category") or "unknown").lower(),
        "fraud_score": float(raw.get("fraud_score", 0.85) or 0.85),
        "risk_band": risk_band,
        "risk_level": risk_band,
        "decision": decision,
        "triggered_rules": triggered_rules,
        "status": str(raw.get("status") or "active"),
        "alerted_at": float(raw.get("alerted_at") or time.time()),
    }


class ConnectionManager:
    """
    WebSocket connection manager for `/ws/alerts` (`MOD-04-FN-05`, `TC-19`).
    Maintains active WebSocket clients, a ring buffer of the last 100 alerts,
    and strict deduplication by `alert_id` and `event_id`.
    """

    def __init__(self, max_recent: int | None = None) -> None:
        self.max_recent = max_recent or settings.ring_buffer_size
        self.active_connections: list[WebSocket] = []
        self.recent_alerts: deque[dict[str, Any]] = deque(maxlen=self.max_recent)
        self._seen_alert_ids: set[str] = set()
        self._seen_event_ids: set[str] = set()
        self.broadcast_count: int = 0
        self.duplicate_dropped_count: int = 0

    def reset(self) -> None:
        self.recent_alerts.clear()
        self._seen_alert_ids.clear()
        self._seen_event_ids.clear()
        self.broadcast_count = 0
        self.duplicate_dropped_count = 0

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        if websocket not in self.active_connections:
            self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    def is_duplicate(self, alert_id: str, event_id: str) -> bool:
        return (alert_id in self._seen_alert_ids) or (event_id in self._seen_event_ids)

    async def broadcast_alert(self, raw_alert: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """
        Normalize, deduplicate by `alert_id`/`event_id`, store in ring buffer,
        and broadcast to all connected WebSocket clients.
        Returns `(was_broadcast, normalized_alert)`.
        """
        alert = normalize_fraud_alert(raw_alert)
        alert_id = alert["alert_id"]
        event_id = alert["event_id"]

        if self.is_duplicate(alert_id, event_id):
            self.duplicate_dropped_count += 1
            return False, alert

        self._seen_alert_ids.add(alert_id)
        self._seen_event_ids.add(event_id)
        self.recent_alerts.appendleft(alert)
        self.broadcast_count += 1

        stale_sockets: list[WebSocket] = []
        for ws in list(self.active_connections):
            try:
                await ws.send_json(alert)
            except Exception:
                stale_sockets.append(ws)

        for ws in stale_sockets:
            self.disconnect(ws)

        return True, alert

    def get_recent_alerts(self, limit: int = 100) -> list[dict[str, Any]]:
        return list(self.recent_alerts)[: max(1, min(limit, self.max_recent))]

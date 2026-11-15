from __future__ import annotations

import asyncio
from typing import Any

import httpx

from ..config import settings


class WebhookAlertHandler:
    """
    Async HTTP webhook dispatcher with exponential backoff retry for `critical` fraud alerts.
    """

    def __init__(self, target_url: str | None = None, max_retries: int = 2) -> None:
        self.target_url = target_url if target_url is not None else settings.webhook_target_url
        self.max_retries = max_retries
        self.dispatched_webhooks: list[dict[str, Any]] = []

    async def dispatch(self, alert: dict[str, Any]) -> dict[str, Any]:
        record: dict[str, Any] = {
            "alert_id": alert.get("alert_id"),
            "event_id": alert.get("event_id"),
            "target_url": self.target_url or "in-memory://soc-webhook",
            "payload": alert,
            "delivered": True,
            "attempts": 1,
        }

        if self.target_url:
            delivered = False
            attempts = 0
            for attempt in range(1, self.max_retries + 1):
                attempts = attempt
                try:
                    async with httpx.AsyncClient(timeout=1.5) as client:
                        resp = await client.post(self.target_url, json=alert)
                        if 200 <= resp.status_code < 300:
                            delivered = True
                            break
                except Exception:
                    if attempt < self.max_retries:
                        await asyncio.sleep(0.05 * attempt)
            record["delivered"] = delivered
            record["attempts"] = attempts

        self.dispatched_webhooks.append(record)
        if len(self.dispatched_webhooks) > 100:
            del self.dispatched_webhooks[:-100]
        return record

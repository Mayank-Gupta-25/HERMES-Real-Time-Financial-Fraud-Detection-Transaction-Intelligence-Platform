from __future__ import annotations

import json
from typing import Any

try:
    from confluent_kafka import Consumer  # type: ignore[import-not-found]
except ImportError:
    Consumer = None  # type: ignore[assignment]

try:
    import redis  # type: ignore[import-not-found]
except ImportError:
    redis = None  # type: ignore[assignment]

from ..config import settings
from ..handlers.email_handler import EmailAlertHandler
from ..handlers.webhook_handler import WebhookAlertHandler
from ..handlers.websocket_handler import ConnectionManager


class FraudAlertConsumer:
    """
    Consumer coordinator reading `FRAUD_ALERT` messages from `hermes.alerts.fraud`
    (and Redis pub/sub `hermes:alerts:stream`), deduplicating by `alert_id`/`event_id`,
    and dispatching to WebSocket `/ws/alerts`, email, and webhook handlers (`MOD-04-FN-05`, `TC-19`).
    """

    def __init__(
        self,
        ws_manager: ConnectionManager,
        email_handler: EmailAlertHandler | None = None,
        webhook_handler: WebhookAlertHandler | None = None,
    ) -> None:
        self.ws_manager = ws_manager
        self.email_handler = email_handler or EmailAlertHandler()
        self.webhook_handler = webhook_handler or WebhookAlertHandler()
        self.processed_count: int = 0
        self.duplicate_count: int = 0

    async def process_alert_message(self, payload: dict[str, Any] | str | bytes) -> dict[str, Any]:
        """
        Parse and dispatch a single fraud alert across WebSocket, Email, and Webhook channels.
        Idempotent on `alert_id` and `event_id`.
        """
        if isinstance(payload, bytes):
            raw_dict = json.loads(payload.decode("utf-8"))
        elif isinstance(payload, str):
            raw_dict = json.loads(payload)
        else:
            raw_dict = dict(payload)

        was_broadcast, alert = await self.ws_manager.broadcast_alert(raw_dict)
        if not was_broadcast:
            self.duplicate_count += 1
            return {
                "dispatched": False,
                "duplicate": True,
                "alert": alert,
            }

        self.processed_count += 1
        risk_band = str(alert.get("risk_band", "high")).lower()

        email_result = None
        webhook_result = None
        if risk_band in ("high", "critical"):
            email_result = await self.email_handler.send_alert_email(alert)
        if risk_band == "critical":
            webhook_result = await self.webhook_handler.dispatch(alert)

        return {
            "dispatched": True,
            "duplicate": False,
            "alert": alert,
            "email_sent": email_result is not None,
            "webhook_sent": webhook_result is not None,
        }

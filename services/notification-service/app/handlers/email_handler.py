from __future__ import annotations

from typing import Any

from ..config import settings


class EmailAlertHandler:
    """
    Structured email notification formatter and dispatcher for high/critical fraud alerts.
    Records formatted emails in-memory for audit and test inspection when an SMTP server is offline.
    """

    def __init__(self, recipient: str | None = None) -> None:
        self.recipient = recipient or settings.alert_email_recipient
        self.sent_emails: list[dict[str, Any]] = []

    def format_email(self, alert: dict[str, Any]) -> dict[str, Any]:
        risk_band = str(alert.get("risk_band", "high")).upper()
        decision = str(alert.get("decision", "flag")).upper()
        amount = float(alert.get("amount", 0.0))
        currency = str(alert.get("currency", "USD"))
        event_id = str(alert.get("event_id", ""))
        alert_id = str(alert.get("alert_id", ""))
        rules = ", ".join(alert.get("triggered_rules") or []) or "ML_ENSEMBLE_THRESHOLD"

        subject = f"[HERMES {risk_band} ALERT - {decision}] {currency} {amount:,.2f} (Event {event_id[:8]})"
        body = (
            f"HERMES Real-Time Fraud Detection Alert\n"
            f"======================================\n"
            f"Alert ID:        {alert_id}\n"
            f"Event ID:        {event_id}\n"
            f"Customer ID:     {alert.get('customer_id')}\n"
            f"Merchant ID:     {alert.get('merchant_id')}\n"
            f"Amount:          {currency} {amount:,.2f}\n"
            f"Fraud Score:     {alert.get('fraud_score')}\n"
            f"Risk Band:       {risk_band}\n"
            f"Decision:        {decision}\n"
            f"Triggered Rules: {rules}\n"
            f"Event Time:      {alert.get('event_time')}\n"
        )
        return {
            "to": self.recipient,
            "subject": subject,
            "body": body,
            "alert_id": alert_id,
            "event_id": event_id,
            "risk_band": alert.get("risk_band", "high"),
        }

    async def send_alert_email(self, alert: dict[str, Any]) -> dict[str, Any]:
        formatted = self.format_email(alert)
        self.sent_emails.append(formatted)
        if len(self.sent_emails) > 100:
            del self.sent_emails[:-100]
        return formatted

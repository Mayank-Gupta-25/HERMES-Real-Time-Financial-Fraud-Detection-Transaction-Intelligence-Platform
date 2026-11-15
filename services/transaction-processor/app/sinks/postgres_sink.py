"""
Idempotent PostgreSQL Operational & Audit Provenance Sink
(`postgres_sink.py` — `STEP AARUSSH-MOD02-03` / `MOD-05-FN-01`, `MOD-05-FN-03`, `TC-20`, `TC-22`).

Persists scored `FraudDecision` records into `transactions` and high/critical risk
alerts into `fraud_alerts` with `ON CONFLICT (event_id) DO UPDATE` idempotency
and full audit provenance (`Lab678_21.pdf` Sections 5 & 6, `NFR-05`, `NFR-07`).
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import psycopg2  # type: ignore[import-untyped]
    HAS_PSYCOPG2 = True
except ImportError:
    psycopg2 = None  # type: ignore[assignment]
    HAS_PSYCOPG2 = False


UPSERT_TRANSACTION_SQL = """
INSERT INTO transactions (
    event_id, event_time, customer_id, merchant_id, amount, currency,
    transaction_type, channel, card_type, category, country, city,
    latitude, longitude, device_id, device_type, device_os, ip_address,
    is_international, status,
    fraud_score, rule_score, ml_score, stat_score, z_score_signal,
    risk_band, decision, triggered_rules, reasons, feature_values,
    model_version, shadow_model_version, shadow_ml_score,
    rule_version, transformation_version, feature_time, decision_time,
    processing_time_ms,
    source_topic, kafka_partition, kafka_offset, ingestion_time,
    storage_destinations, schema_version
) VALUES (
    %(event_id)s, %(event_time)s, %(customer_id)s, %(merchant_id)s, %(amount)s, %(currency)s,
    %(transaction_type)s, %(channel)s, %(card_type)s, %(category)s, %(country)s, %(city)s,
    %(latitude)s, %(longitude)s, %(device_id)s, %(device_type)s, %(device_os)s, %(ip_address)s,
    %(is_international)s, %(status)s,
    %(fraud_score)s, %(rule_score)s, %(ml_score)s, %(stat_score)s, %(z_score_signal)s,
    %(risk_band)s, %(decision)s, %(triggered_rules)s::jsonb, %(reasons)s::jsonb, %(feature_values)s::jsonb,
    %(model_version)s, %(shadow_model_version)s, %(shadow_ml_score)s,
    %(rule_version)s, %(transformation_version)s, %(feature_time)s, %(decision_time)s,
    %(processing_time_ms)s,
    %(source_topic)s, %(kafka_partition)s, %(kafka_offset)s, %(ingestion_time)s,
    %(storage_destinations)s::jsonb, %(schema_version)s
)
ON CONFLICT (event_id) DO UPDATE SET
    fraud_score = EXCLUDED.fraud_score,
    rule_score = EXCLUDED.rule_score,
    ml_score = EXCLUDED.ml_score,
    stat_score = EXCLUDED.stat_score,
    z_score_signal = EXCLUDED.z_score_signal,
    risk_band = EXCLUDED.risk_band,
    decision = EXCLUDED.decision,
    triggered_rules = EXCLUDED.triggered_rules,
    reasons = EXCLUDED.reasons,
    feature_values = EXCLUDED.feature_values,
    model_version = EXCLUDED.model_version,
    shadow_model_version = EXCLUDED.shadow_model_version,
    shadow_ml_score = EXCLUDED.shadow_ml_score,
    rule_version = EXCLUDED.rule_version,
    transformation_version = EXCLUDED.transformation_version,
    feature_time = EXCLUDED.feature_time,
    decision_time = EXCLUDED.decision_time,
    processing_time_ms = EXCLUDED.processing_time_ms,
    storage_destinations = EXCLUDED.storage_destinations;
"""

UPSERT_FRAUD_ALERT_SQL = """
INSERT INTO fraud_alerts (
    alert_id, event_id, event_time, customer_id, merchant_id,
    amount, currency, category, fraud_score, risk_band, decision,
    triggered_rules, status
) VALUES (
    %(alert_id)s, %(event_id)s, %(event_time)s, %(customer_id)s, %(merchant_id)s,
    %(amount)s, %(currency)s, %(category)s, %(fraud_score)s, %(risk_band)s, %(decision)s,
    %(triggered_rules)s::jsonb, %(status)s
)
ON CONFLICT (event_id) DO UPDATE SET
    fraud_score = EXCLUDED.fraud_score,
    risk_band = EXCLUDED.risk_band,
    decision = EXCLUDED.decision,
    triggered_rules = EXCLUDED.triggered_rules,
    updated_at = NOW();
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PostgresOperationalSink:
    """
    Idempotent operational sink for PostgreSQL `transactions` and `fraud_alerts`.
    Maintains an in-memory replica alongside optional database cursor/connection
    execution so unit and integration tests can inspect persisted rows directly.
    """

    def __init__(self, db_connection: Optional[Any] = None) -> None:
        self.db_connection = db_connection
        self.transactions_by_event_id: Dict[str, Dict[str, Any]] = {}
        self.alerts_by_event_id: Dict[str, Dict[str, Any]] = {}
        self.upsert_calls_count: int = 0
        self.alert_upsert_calls_count: int = 0

    @staticmethod
    def build_transaction_row(decision_record: Dict[str, Any]) -> Dict[str, Any]:
        """Map a `FraudDecision` dictionary to the exact `transactions` table row schema."""
        event_id = str(decision_record["event_id"]).strip().lower()
        loc = decision_record.get("location") or {}
        dev = decision_record.get("device") or {}
        prov = decision_record.get("provenance") or {}
        decision = str(decision_record.get("decision", "approve")).lower()

        status_val = str(decision_record.get("status") or "")
        if not status_val or status_val == "pending":
            status_val = (
                "declined"
                if decision == "block"
                else ("flagged" if decision == "flag" else "completed")
            )

        storage_destinations = prov.get(
            "storage_destinations",
            [
                "postgres:transactions",
                "redis:customer_state",
                "minio:parquet",
            ]
            + (["postgres:fraud_alerts"] if decision in ("flag", "block") else []),
        )

        event_time = str(decision_record.get("event_time") or _utc_now_iso())
        feature_time = str(decision_record.get("feature_time") or event_time)
        decision_time = str(decision_record.get("decision_time") or event_time)
        ingestion_time = str(prov.get("ingestion_time") or event_time)

        stat_val = float(
            decision_record.get("stat_score")
            if decision_record.get("stat_score") is not None
            else (decision_record.get("z_score_signal") or 0.0)
        )

        triggered_rules = list(decision_record.get("triggered_rules", []))
        reasons = list(decision_record.get("reasons", []))
        feature_values = dict(decision_record.get("feature_values", {}))

        return {
            "event_id": event_id,
            "event_time": event_time,
            "customer_id": str(decision_record["customer_id"]).strip().lower(),
            "merchant_id": str(decision_record["merchant_id"]).strip().lower(),
            "amount": round(float(decision_record["amount"]), 2),
            "currency": str(decision_record.get("currency", "USD")).upper(),
            "transaction_type": str(decision_record.get("transaction_type", "purchase")).lower(),
            "channel": str(decision_record.get("channel", "online")).lower(),
            "card_type": str(decision_record.get("card_type", "credit")).lower(),
            "category": str(decision_record.get("category", "retail")).lower(),
            "country": str(loc.get("country", decision_record.get("country", "US"))).upper(),
            "city": str(loc.get("city", decision_record.get("city", "Unknown"))),
            "latitude": float(loc.get("latitude", 0.0)),
            "longitude": float(loc.get("longitude", 0.0)),
            "device_id": dev.get("device_id") if isinstance(dev, dict) else None,
            "device_type": dev.get("device_type") if isinstance(dev, dict) else None,
            "device_os": dev.get("os") if isinstance(dev, dict) else None,
            "ip_address": dev.get("ip_address") if isinstance(dev, dict) else None,
            "is_international": bool(
                decision_record.get(
                    "is_international",
                    bool(feature_values.get("is_international", False)),
                )
            ),
            "status": status_val,
            "fraud_score": float(decision_record.get("fraud_score", 0.0)),
            "rule_score": float(decision_record.get("rule_score", 0.0)),
            "ml_score": float(decision_record.get("ml_score", 0.0)),
            "stat_score": stat_val,
            "z_score_signal": float(decision_record.get("z_score_signal") or stat_val),
            "risk_band": str(decision_record.get("risk_band", "low")).lower(),
            "decision": decision,
            "triggered_rules": triggered_rules,
            "reasons": reasons,
            "feature_values": feature_values,
            "model_version": str(decision_record.get("model_version", "xgboost-v1.0.0")),
            "shadow_model_version": decision_record.get("shadow_model_version"),
            "shadow_ml_score": (
                float(decision_record["shadow_ml_score"])
                if decision_record.get("shadow_ml_score") is not None
                else None
            ),
            "rule_version": str(decision_record.get("rule_version", "rules-v1.0.0")),
            "transformation_version": str(
                decision_record.get("transformation_version", "features-v1.0.0")
            ),
            "feature_time": feature_time,
            "decision_time": decision_time,
            "processing_time_ms": int(round(float(decision_record.get("processing_time_ms", 0) or 0))),
            "source_topic": str(prov.get("source_topic", "hermes.transactions.raw")),
            "kafka_partition": int(prov.get("partition", prov.get("kafka_partition", 0)) or 0),
            "kafka_offset": int(prov.get("offset", prov.get("kafka_offset", 0)) or 0),
            "ingestion_time": ingestion_time,
            "storage_destinations": list(storage_destinations),
            "schema_version": str(decision_record.get("schema_version", "1.0")),
        }

    @staticmethod
    def build_alert_row(decision_or_alert: Dict[str, Any]) -> Dict[str, Any]:
        """Map a flagged/blocked `FraudDecision` or `FraudAlert` to `fraud_alerts` columns."""
        event_id = str(decision_or_alert["event_id"]).strip().lower()
        alert_id = str(decision_or_alert.get("alert_id") or f"alert-{event_id}")
        raw_status = str(decision_or_alert.get("alert_status") or decision_or_alert.get("status") or "active").lower()
        alert_status = raw_status if raw_status in ("active", "acknowledged", "dismissed") else "active"
        return {
            "alert_id": alert_id,
            "event_id": event_id,
            "event_time": str(decision_or_alert.get("event_time") or _utc_now_iso()),
            "customer_id": str(decision_or_alert["customer_id"]).strip().lower(),
            "merchant_id": str(decision_or_alert["merchant_id"]).strip().lower(),
            "amount": round(float(decision_or_alert["amount"]), 2),
            "currency": str(decision_or_alert.get("currency", "USD")).upper(),
            "category": str(decision_or_alert.get("category", "retail")).lower(),
            "fraud_score": float(decision_or_alert.get("fraud_score", 0.0)),
            "risk_band": str(decision_or_alert.get("risk_band", "high")).lower(),
            "decision": str(decision_or_alert.get("decision", "flag")).lower(),
            "triggered_rules": list(decision_or_alert.get("triggered_rules", [])),
            "status": alert_status,
        }

    def upsert_decision(
        self, decision_record: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Idempotently upsert a single `FraudDecision` into `transactions`
        (and `fraud_alerts` if `decision in ('flag', 'block')`).
        """
        tx_row = self.build_transaction_row(decision_record)
        event_id = tx_row["event_id"]
        self.transactions_by_event_id[event_id] = tx_row
        self.upsert_calls_count += 1

        alert_row: Optional[Dict[str, Any]] = None
        if tx_row["decision"] in ("flag", "block"):
            alert_row = self.build_alert_row(tx_row)
            self.alerts_by_event_id[event_id] = alert_row
            self.alert_upsert_calls_count += 1

        if self.db_connection is not None:
            sql_params = dict(tx_row)
            sql_params["triggered_rules"] = json.dumps(tx_row["triggered_rules"])
            sql_params["reasons"] = json.dumps(tx_row["reasons"])
            sql_params["feature_values"] = json.dumps(tx_row["feature_values"])
            sql_params["storage_destinations"] = json.dumps(tx_row["storage_destinations"])

            with self.db_connection.cursor() as cur:
                cur.execute(UPSERT_TRANSACTION_SQL, sql_params)
                if alert_row is not None:
                    alert_sql_params = dict(alert_row)
                    alert_sql_params["triggered_rules"] = json.dumps(alert_row["triggered_rules"])
                    cur.execute(UPSERT_FRAUD_ALERT_SQL, alert_sql_params)
            if hasattr(self.db_connection, "commit"):
                self.db_connection.commit()

        return tx_row, alert_row

    def upsert_batch(
        self, decision_records: Iterable[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Idempotently upsert a batch of `FraudDecision` records."""
        tx_rows: List[Dict[str, Any]] = []
        alert_rows: List[Dict[str, Any]] = []
        for rec in decision_records:
            tx_row, alert_row = self.upsert_decision(rec)
            tx_rows.append(tx_row)
            if alert_row is not None:
                alert_rows.append(alert_row)
        return tx_rows, alert_rows

    def get_transaction(self, event_id: str) -> Optional[Dict[str, Any]]:
        return self.transactions_by_event_id.get(event_id.strip().lower())

    def get_alert_by_event_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        return self.alerts_by_event_id.get(event_id.strip().lower())

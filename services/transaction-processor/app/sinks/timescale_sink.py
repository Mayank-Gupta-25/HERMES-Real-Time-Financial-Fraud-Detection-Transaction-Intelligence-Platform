"""
TimescaleDB Hypertable Metrics Sink (`timescale_sink.py` — `STEP AARUSSH-MOD02-03` / `MOD-05-FN-02`, `TC-21`).

Writes time-bucketed metrics into the 3 TimescaleDB hypertables defined in
`infrastructure/timescaledb/init.sql`:
1. `transaction_metrics` (`PRIMARY KEY (bucket_time, window_size, channel, region)`)
2. `category_metrics` (`PRIMARY KEY (bucket_time, category)`)
3. `geo_metrics` (`PRIMARY KEY (bucket_time, country, city)`)
"""
from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any, Dict, Iterable, List, Optional, Tuple


UPSERT_TRANSACTION_METRICS_SQL = """
INSERT INTO transaction_metrics (
    bucket_time, window_size, channel, region,
    tx_count, tps, total_amount, avg_amount, max_amount, p95_amount,
    fraud_count, flagged_count, blocked_count, fraud_rate,
    p95_latency_ms, unique_customers, unique_merchants
) VALUES (
    %(bucket_time)s, %(window_size)s, %(channel)s, %(region)s,
    %(tx_count)s, %(tps)s, %(total_amount)s, %(avg_amount)s, %(max_amount)s, %(p95_amount)s,
    %(fraud_count)s, %(flagged_count)s, %(blocked_count)s, %(fraud_rate)s,
    %(p95_latency_ms)s, %(unique_customers)s, %(unique_merchants)s
)
ON CONFLICT (bucket_time, window_size, channel, region) DO UPDATE SET
    tx_count = EXCLUDED.tx_count,
    tps = EXCLUDED.tps,
    total_amount = EXCLUDED.total_amount,
    avg_amount = EXCLUDED.avg_amount,
    max_amount = EXCLUDED.max_amount,
    p95_amount = EXCLUDED.p95_amount,
    fraud_count = EXCLUDED.fraud_count,
    flagged_count = EXCLUDED.flagged_count,
    blocked_count = EXCLUDED.blocked_count,
    fraud_rate = EXCLUDED.fraud_rate,
    p95_latency_ms = EXCLUDED.p95_latency_ms,
    unique_customers = EXCLUDED.unique_customers,
    unique_merchants = EXCLUDED.unique_merchants;
"""

UPSERT_CATEGORY_METRICS_SQL = """
INSERT INTO category_metrics (
    bucket_time, category, tx_count, total_amount, fraud_count
) VALUES (
    %(bucket_time)s, %(category)s, %(tx_count)s, %(total_amount)s, %(fraud_count)s
)
ON CONFLICT (bucket_time, category) DO UPDATE SET
    tx_count = EXCLUDED.tx_count,
    total_amount = EXCLUDED.total_amount,
    fraud_count = EXCLUDED.fraud_count;
"""

UPSERT_GEO_METRICS_SQL = """
INSERT INTO geo_metrics (
    bucket_time, country, city, latitude, longitude,
    tx_count, total_amount, fraud_count
) VALUES (
    %(bucket_time)s, %(country)s, %(city)s, %(latitude)s, %(longitude)s,
    %(tx_count)s, %(total_amount)s, %(fraud_count)s
)
ON CONFLICT (bucket_time, country, city) DO UPDATE SET
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    tx_count = EXCLUDED.tx_count,
    total_amount = EXCLUDED.total_amount,
    fraud_count = EXCLUDED.fraud_count;
"""


def _floor_to_minute_utc(iso_str: str) -> str:
    raw = iso_str.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    floored = dt.replace(second=0, microsecond=0)
    return floored.isoformat().replace("+00:00", "Z")


def _window_seconds(window_size: str) -> float:
    mapping = {"1m": 60.0, "5m": 300.0, "1h": 3600.0}
    return mapping.get(window_size, 60.0)


class TimescaleMetricsSink:
    """
    Sink for TimescaleDB `transaction_metrics`, `category_metrics`, and `geo_metrics`
    hypertables, with an in-memory store for deterministic unit/integration testing.
    """

    def __init__(self, db_connection: Optional[Any] = None) -> None:
        self.db_connection = db_connection
        self.transaction_metrics: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
        self.category_metrics: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.geo_metrics: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._seen_decision_ids: set[str] = set()
        self._bucket_decisions: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = {}
        self._cat_decisions: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        self._geo_decisions: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}

    def write_transaction_metric(self, metric: Dict[str, Any]) -> Dict[str, Any]:
        bucket_time = str(metric["bucket_time"])
        window_size = str(metric.get("window_size", "1m"))
        channel = str(metric.get("channel", "all"))
        region = str(metric.get("region", "all"))

        tx_count = int(metric.get("tx_count", 0))
        fraud_count = int(metric.get("fraud_count", 0))
        flagged_count = int(metric.get("flagged_count", 0))
        blocked_count = int(metric.get("blocked_count", 0))
        if fraud_count == 0 and (flagged_count + blocked_count) > 0:
            fraud_count = flagged_count + blocked_count

        total_amount = round(float(metric.get("total_amount", 0.0)), 2)
        avg_amount = round(
            float(metric.get("avg_amount", (total_amount / tx_count) if tx_count > 0 else 0.0)),
            2,
        )
        fraud_rate = round(
            float(metric.get("fraud_rate", (fraud_count / tx_count) if tx_count > 0 else 0.0)),
            4,
        )
        tps = round(
            float(metric.get("tps", tx_count / _window_seconds(window_size))),
            4,
        )

        row = {
            "bucket_time": bucket_time,
            "window_size": window_size,
            "channel": channel,
            "region": region,
            "tx_count": tx_count,
            "tps": tps,
            "total_amount": total_amount,
            "avg_amount": avg_amount,
            "max_amount": round(float(metric.get("max_amount", avg_amount)), 2),
            "p95_amount": round(float(metric.get("p95_amount", avg_amount)), 2),
            "fraud_count": fraud_count,
            "flagged_count": flagged_count,
            "blocked_count": blocked_count,
            "fraud_rate": fraud_rate,
            "p95_latency_ms": round(float(metric.get("p95_latency_ms", 0.0)), 2),
            "unique_customers": int(metric.get("unique_customers", 0)),
            "unique_merchants": int(metric.get("unique_merchants", 0)),
        }

        key = (bucket_time, window_size, channel, region)
        self.transaction_metrics[key] = row

        if self.db_connection is not None:
            with self.db_connection.cursor() as cur:
                cur.execute(UPSERT_TRANSACTION_METRICS_SQL, row)
            if hasattr(self.db_connection, "commit"):
                self.db_connection.commit()

        return row

    def write_category_metric(self, metric: Dict[str, Any]) -> Dict[str, Any]:
        bucket_time = str(metric["bucket_time"])
        category = str(metric["category"]).lower()
        row = {
            "bucket_time": bucket_time,
            "category": category,
            "tx_count": int(metric.get("tx_count", 0)),
            "total_amount": round(float(metric.get("total_amount", 0.0)), 2),
            "fraud_count": int(metric.get("fraud_count", 0)),
        }
        self.category_metrics[(bucket_time, category)] = row

        if self.db_connection is not None:
            with self.db_connection.cursor() as cur:
                cur.execute(UPSERT_CATEGORY_METRICS_SQL, row)
            if hasattr(self.db_connection, "commit"):
                self.db_connection.commit()

        return row

    def write_geo_metric(self, metric: Dict[str, Any]) -> Dict[str, Any]:
        bucket_time = str(metric["bucket_time"])
        country = str(metric["country"]).upper()
        city = str(metric["city"])
        row = {
            "bucket_time": bucket_time,
            "country": country,
            "city": city,
            "latitude": float(metric.get("latitude", 0.0)),
            "longitude": float(metric.get("longitude", 0.0)),
            "tx_count": int(metric.get("tx_count", 0)),
            "total_amount": round(float(metric.get("total_amount", 0.0)), 2),
            "fraud_count": int(metric.get("fraud_count", 0)),
        }
        self.geo_metrics[(bucket_time, country, city)] = row

        if self.db_connection is not None:
            with self.db_connection.cursor() as cur:
                cur.execute(UPSERT_GEO_METRICS_SQL, row)
            if hasattr(self.db_connection, "commit"):
                self.db_connection.commit()

        return row

    def ingest_scored_decisions(
        self,
        decisions: Iterable[Dict[str, Any]],
        window_size: str = "1m",
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Aggregate a batch of scored `FraudDecision` records into 1-minute buckets
        across `transaction_metrics`, `category_metrics`, and `geo_metrics`
        while suppressing duplicate `event_id`s.
        """
        unique_decisions: List[Dict[str, Any]] = []
        for dec in decisions:
            ev_id = str(dec.get("event_id", ""))
            if ev_id and ev_id in self._seen_decision_ids:
                continue
            if ev_id:
                self._seen_decision_ids.add(ev_id)
            unique_decisions.append(dec)

        touched_buckets: set[Tuple[str, str, str, str]] = set()
        touched_cats: set[Tuple[str, str]] = set()
        touched_geos: set[Tuple[str, str, str]] = set()

        for dec in unique_decisions:
            bucket_time = _floor_to_minute_utc(str(dec["event_time"]))
            b_key = (bucket_time, window_size, "all", "all")
            self._bucket_decisions.setdefault(b_key, []).append(dec)
            touched_buckets.add(b_key)

            category = str(dec.get("category", "retail")).lower()
            c_key = (bucket_time, category)
            self._cat_decisions.setdefault(c_key, []).append(dec)
            touched_cats.add(c_key)

            loc = dec.get("location") or {}
            country = str(loc.get("country", "US")).upper()
            city = str(loc.get("city", "Unknown"))
            g_key = (bucket_time, country, city)
            self._geo_decisions.setdefault(g_key, []).append(dec)
            touched_geos.add(g_key)

        tx_rows: List[Dict[str, Any]] = []
        for bucket_key in sorted(touched_buckets):
            items = self._bucket_decisions[bucket_key]
            bucket_time, win_size, channel, region = bucket_key
            amounts = sorted(float(x["amount"]) for x in items)
            latencies = sorted(float(x.get("processing_time_ms", 0.0) or 0.0) for x in items)
            n = len(items)
            total_amt = sum(amounts)
            p95_idx = min(n - 1, max(0, int(math.ceil(0.95 * n)) - 1))
            flagged = sum(1 for x in items if str(x.get("decision")) == "flag")
            blocked = sum(1 for x in items if str(x.get("decision")) == "block")
            fraud_cnt = flagged + blocked

            tx_rows.append(
                self.write_transaction_metric(
                    {
                        "bucket_time": bucket_time,
                        "window_size": win_size,
                        "channel": channel,
                        "region": region,
                        "tx_count": n,
                        "tps": round(n / _window_seconds(win_size), 4),
                        "total_amount": round(total_amt, 2),
                        "avg_amount": round(total_amt / n, 2),
                        "max_amount": round(amounts[-1], 2),
                        "p95_amount": round(amounts[p95_idx], 2),
                        "fraud_count": fraud_cnt,
                        "flagged_count": flagged,
                        "blocked_count": blocked,
                        "fraud_rate": round(fraud_cnt / n, 4),
                        "p95_latency_ms": round(latencies[p95_idx], 2),
                        "unique_customers": len({str(x["customer_id"]) for x in items}),
                        "unique_merchants": len({str(x["merchant_id"]) for x in items}),
                    }
                )
            )

        cat_rows: List[Dict[str, Any]] = []
        for cat_key in sorted(touched_cats):
            items = self._cat_decisions[cat_key]
            bucket_time, category = cat_key
            fraud_cnt = sum(1 for x in items if str(x.get("decision")) in ("flag", "block"))
            cat_rows.append(
                self.write_category_metric(
                    {
                        "bucket_time": bucket_time,
                        "category": category,
                        "tx_count": len(items),
                        "total_amount": round(sum(float(x["amount"]) for x in items), 2),
                        "fraud_count": fraud_cnt,
                    }
                )
            )

        geo_rows: List[Dict[str, Any]] = []
        for geo_key in sorted(touched_geos):
            items = self._geo_decisions[geo_key]
            bucket_time, country, city = geo_key
            loc0 = items[0].get("location") or {}
            fraud_cnt = sum(1 for x in items if str(x.get("decision")) in ("flag", "block"))
            geo_rows.append(
                self.write_geo_metric(
                    {
                        "bucket_time": bucket_time,
                        "country": country,
                        "city": city,
                        "latitude": float(loc0.get("latitude", 0.0)),
                        "longitude": float(loc0.get("longitude", 0.0)),
                        "tx_count": len(items),
                        "total_amount": round(sum(float(x["amount"]) for x in items), 2),
                        "fraud_count": fraud_cnt,
                    }
                )
            )

        return {
            "transaction_metrics": tx_rows,
            "category_metrics": cat_rows,
            "geo_metrics": geo_rows,
        }

"""
Static Event-Time Window Aggregations (`1 min`, `5 min`, `1 h`) for HERMES (`MOD-02-FN-02`).

Per `Lab678_21.pdf` Section 5:
  - Uses static event-time windows (`1m` = 60s, `5m` = 300s, `1h` = 3600s)
    plus rule-support horizons (`10m` = 600s for card-testing micro-transactions,
    `24h` = 86400s for AML structuring transactions).
  - Handles out-of-order events deterministically within the bounded watermark
    delay (`watermark_delay_sec = 5.0`).
  - Emits system-wide 1-minute tumbling bucket summaries (`WindowMetricEvent`)
    for `hermes.metrics.realtime` and TimescaleDB persistence.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

STATIC_WINDOWS_SECONDS: Dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "1h": 3600,
}

MICRO_TXN_WINDOW_SECONDS: int = 600  # 10m for card testing
STRUCTURING_WINDOW_SECONDS: int = 86400  # 24h for AML structuring


def parse_event_time_epoch(value: Any) -> float:
    """Parse an ISO-8601 UTC string, datetime, or numeric epoch into UTC epoch seconds."""
    if isinstance(value, (int, float)):
        val = float(value)
        # Convert milliseconds to seconds if epoch > 1e11
        return val / 1000.0 if val > 1e11 else val
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    raise ValueError(f"Cannot parse event_time: {value!r}")


def format_epoch_utc_z(epoch_sec: float) -> str:
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class WindowEventRecord:
    event_id: str
    customer_id: str
    merchant_id: str
    amount: float
    event_time_epoch: float
    channel: str = "online"
    country: str = "US"


@dataclass
class TumblingBucketState:
    bucket_start_epoch: int
    window_size: str = "1m"
    window_seconds: int = 60
    amounts: List[float] = field(default_factory=list)
    customers: Set[str] = field(default_factory=set)
    merchants: Set[str] = field(default_factory=set)
    event_ids: Set[str] = field(default_factory=set)
    channel: str = "all"
    region: str = "global"

    def record(self, ev: WindowEventRecord) -> None:
        if ev.event_id in self.event_ids:
            return
        self.event_ids.add(ev.event_id)
        self.amounts.append(ev.amount)
        self.customers.add(ev.customer_id)
        self.merchants.add(ev.merchant_id)

    def to_metric_event(self) -> Dict[str, Any]:
        tx_count = len(self.amounts)
        total_amount = round(sum(self.amounts), 2) if tx_count > 0 else 0.0
        avg_amount = round(total_amount / tx_count, 2) if tx_count > 0 else 0.0
        max_amount = round(max(self.amounts), 2) if tx_count > 0 else 0.0
        if tx_count > 0:
            sorted_amts = sorted(self.amounts)
            idx = min(tx_count - 1, max(0, int(math.ceil(0.95 * tx_count)) - 1))
            p95_amount = round(sorted_amts[idx], 2)
        else:
            p95_amount = 0.0

        return {
            "bucket_time": format_epoch_utc_z(float(self.bucket_start_epoch)),
            "window_start": format_epoch_utc_z(float(self.bucket_start_epoch)),
            "window_end": format_epoch_utc_z(float(self.bucket_start_epoch + self.window_seconds)),
            "window_size": self.window_size,
            "channel": self.channel,
            "region": self.region,
            "tx_count": tx_count,
            "tps": round(tx_count / float(self.window_seconds), 4),
            "total_amount": total_amount,
            "avg_amount": avg_amount,
            "max_amount": max_amount,
            "p95_amount": p95_amount,
            "unique_customers": len(self.customers),
            "unique_merchants": len(self.merchants),
        }


class EventTimeWindowAggregator:
    """
    Stateful event-time window aggregator for `1m` (60s), `5m` (300s), and `1h` (3600s)
    per-customer sliding windows and system-wide 1-minute tumbling buckets.
    """

    def __init__(self, watermark_delay_sec: float = 5.0) -> None:
        self.watermark_delay_sec = float(watermark_delay_sec)
        self.max_event_time_epoch: float = 0.0
        self.current_watermark_epoch: float = 0.0
        self._customer_events: Dict[str, List[WindowEventRecord]] = {}
        self._customer_seen_ids: Dict[str, Set[str]] = {}
        self._buckets_1m: Dict[int, TumblingBucketState] = {}
        self.late_events_beyond_watermark: int = 0

    def _extract_record(self, event: Dict[str, Any]) -> WindowEventRecord:
        event_id = str(event.get("event_id") or event.get("transaction_id") or "")
        customer_id = str(event["customer_id"])
        merchant_id = str(event.get("merchant_id") or "unknown")
        amount = float(event["amount"])
        raw_time = event.get("event_time") or event.get("timestamp") or event.get("event_time_epoch")
        event_time_epoch = parse_event_time_epoch(raw_time)
        channel = str(event.get("channel") or "online")
        loc = event.get("location") or {}
        country = str(loc.get("country") or event.get("country") or "US")
        return WindowEventRecord(
            event_id=event_id,
            customer_id=customer_id,
            merchant_id=merchant_id,
            amount=amount,
            event_time_epoch=event_time_epoch,
            channel=channel,
            country=country,
        )

    def process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ingest a transaction event into the per-customer event-time window state
        and return the computed `1m`, `5m`, and `1h` window aggregations as of
         the event's `event_time`.
        """
        rec = self._extract_record(event)
        t = rec.event_time_epoch

        is_out_of_order = self.max_event_time_epoch > 0.0 and t < self.max_event_time_epoch
        is_late_beyond_watermark = (
            self.max_event_time_epoch > 0.0 and t < self.current_watermark_epoch
        )
        if is_late_beyond_watermark:
            self.late_events_beyond_watermark += 1

        if t > self.max_event_time_epoch:
            self.max_event_time_epoch = t
            self.current_watermark_epoch = self.max_event_time_epoch - self.watermark_delay_sec

        cust_seen = self._customer_seen_ids.setdefault(rec.customer_id, set())
        cust_list = self._customer_events.setdefault(rec.customer_id, [])

        if rec.event_id not in cust_seen:
            cust_seen.add(rec.event_id)
            cust_list.append(rec)
            cust_list.sort(key=lambda r: r.event_time_epoch)

        # Prune records older than 24h before the earliest active watermark/query time
        min_keep_epoch = min(t, self.current_watermark_epoch) - float(STRUCTURING_WINDOW_SECONDS + 60)
        if len(cust_list) > 64:
            pruned: List[WindowEventRecord] = []
            for r in cust_list:
                if r.event_time_epoch >= min_keep_epoch:
                    pruned.append(r)
                else:
                    cust_seen.discard(r.event_id)
            self._customer_events[rec.customer_id] = pruned
            cust_list = pruned

        # Update 1-minute tumbling bucket
        bucket_start = int(math.floor(t / 60.0) * 60)
        bucket = self._buckets_1m.get(bucket_start)
        if bucket is None:
            bucket = TumblingBucketState(bucket_start_epoch=bucket_start)
            self._buckets_1m[bucket_start] = bucket
        bucket.record(rec)

        return self.compute_customer_windows(
            customer_id=rec.customer_id,
            as_of_epoch=t,
            is_out_of_order=is_out_of_order,
            is_late_beyond_watermark=is_late_beyond_watermark,
        )

    def compute_customer_windows(
        self,
        customer_id: str,
        as_of_epoch: float,
        is_out_of_order: bool = False,
        is_late_beyond_watermark: bool = False,
    ) -> Dict[str, Any]:
        """
        Compute static `1m` (60s), `5m` (300s), and `1h` (3600s) window metrics
        over `[as_of_epoch - window_sec, as_of_epoch]`.
        """
        cust_list = self._customer_events.get(customer_id, [])
        t = float(as_of_epoch)

        events_1m = [r for r in cust_list if (t - 60.0) <= r.event_time_epoch <= t]
        events_5m = [r for r in cust_list if (t - 300.0) <= r.event_time_epoch <= t]
        events_1h = [r for r in cust_list if (t - 3600.0) <= r.event_time_epoch <= t]
        events_10m_micro = [
            r for r in cust_list if (t - 600.0) <= r.event_time_epoch <= t and r.amount <= 5.0
        ]
        events_24h_struct = [
            r
            for r in cust_list
            if (t - 86400.0) <= r.event_time_epoch <= t and 8000.0 <= r.amount < 10000.0
        ]

        txn_count_1m = len(events_1m)
        txn_count_5m = len(events_5m)
        txn_count_1h = len(events_1h)

        amount_sum_1m = round(sum(r.amount for r in events_1m), 2)
        amount_sum_5m = round(sum(r.amount for r in events_5m), 2)
        amount_sum_1h = round(sum(r.amount for r in events_1h), 2)
        amount_avg_1h = round(amount_sum_1h / txn_count_1h, 2) if txn_count_1h > 0 else 0.0
        amount_max_1h = round(max((r.amount for r in events_1h), default=0.0), 2)
        unique_merchants_1h = len({r.merchant_id for r in events_1h})

        return {
            "customer_id": customer_id,
            "as_of_epoch": t,
            "watermark_epoch": self.current_watermark_epoch,
            "is_out_of_order": is_out_of_order,
            "is_late_beyond_watermark": is_late_beyond_watermark,
            "txn_count_1m": txn_count_1m,
            "txn_count_5m": txn_count_5m,
            "txn_count_1h": txn_count_1h,
            "amount_sum_1m": amount_sum_1m,
            "amount_sum_5m": amount_sum_5m,
            "amount_sum_1h": amount_sum_1h,
            "amount_avg_1h": amount_avg_1h,
            "amount_max_1h": amount_max_1h,
            "unique_merchants_1h": unique_merchants_1h,
            "micro_txn_count_10m": len(events_10m_micro),
            "structuring_txn_count_24h": len(events_24h_struct),
        }

    def get_bucket_metrics(self) -> List[Dict[str, Any]]:
        """Return all 1-minute tumbling bucket metrics ordered by `bucket_time`."""
        return [
            self._buckets_1m[k].to_metric_event()
            for k in sorted(self._buckets_1m.keys())
        ]

    def flush_closed_buckets(self) -> List[Dict[str, Any]]:
        """Flush and return 1-minute buckets whose window_end <= current_watermark_epoch."""
        closed_keys = [
            k
            for k in sorted(self._buckets_1m.keys())
            if (k + 60) <= self.current_watermark_epoch
        ]
        results = [self._buckets_1m.pop(k).to_metric_event() for k in closed_keys]
        return results

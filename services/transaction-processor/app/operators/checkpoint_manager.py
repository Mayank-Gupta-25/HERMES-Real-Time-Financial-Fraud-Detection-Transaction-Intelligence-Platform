"""
Durable Flink Checkpoint Manager & State Recovery Operator
(`checkpoint_manager.py` — `STEP AARUSSH-MOD08-01` / `MOD-08-FN-02`, `TC-27`).

Implements atomic state snapshotting and deterministic recovery for the HERMES
streaming pipeline (`Lab678_21.pdf` Section 5, `NFR-03`, `NFR-04`, `NFR-07`):
- Snapshots `checkpoint_id`, Kafka `committed_offsets` (`offsets`), deduplication
  `seen_event_ids`, event-time watermark state, per-customer Welford baselines,
  and sliding window buffers to durable storage (`checkpoint_dir` + optional Redis
  `flink:checkpoint:latest`).
- Restores the latest committed checkpoint on worker recovery, recording
  `checkpoint_id`, `committed_offsets`, `restore_outcome`, and `recovery_time_ms`.
"""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import time
from typing import Any, Dict, List, Optional

from ..jobs.aggregation_job import (
    EventTimeWindowAggregator,
    TumblingBucketState,
    WindowEventRecord,
)
from ..operators.dedup_and_clean import DeduplicationAndCleaningOperator
from ..operators.feature_extractor import CustomerBaselineState, FeatureExtractor

try:
    from prometheus_client import Gauge, REGISTRY  # type: ignore[import-untyped]
    HAS_PROMETHEUS = True
except ImportError:
    Gauge = None  # type: ignore[assignment]
    REGISTRY = None  # type: ignore[assignment]
    HAS_PROMETHEUS = False


def _get_or_create_gauge(name: str, doc: str, labelnames: List[str]) -> Any:
    if not HAS_PROMETHEUS or Gauge is None or REGISTRY is None:
        return None
    existing = REGISTRY._names_to_collectors.get(name)
    if existing is not None:
        return existing
    return Gauge(name, doc, labelnames=labelnames)


CHECKPOINT_DURATION_GAUGE = _get_or_create_gauge(
    "hermes_flink_last_checkpoint_duration_ms",
    "Latest Flink checkpoint duration in milliseconds",
    ["job_name"],
)
RECOVERY_TIME_GAUGE = _get_or_create_gauge(
    "hermes_flink_last_recovery_time_ms",
    "Latest Flink checkpoint restore recovery time in milliseconds",
    ["job_name"],
)


class CheckpointManager:
    """
    Manages periodic durable checkpoints and state restoration for the HERMES
    transaction-processor pipeline.
    """

    def __init__(
        self,
        checkpoint_dir: Optional[str | Path] = None,
        redis_client: Optional[Any] = None,
        checkpoint_interval_ms: int = 5000,
        job_name: str = "hermes-transaction-processor",
    ) -> None:
        if checkpoint_dir is None:
            self.checkpoint_dir = Path(tempfile.gettempdir()) / "hermes_flink_checkpoints"
        else:
            self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        self.redis_client = redis_client
        self.checkpoint_interval_ms = int(checkpoint_interval_ms)
        self.job_name = job_name
        self._sequence: int = 0
        self.last_checkpoint_id: Optional[str] = None
        self.last_checkpoint_duration_ms: float = 0.0
        self.last_recovery_telemetry: Optional[Dict[str, Any]] = None

    def save_checkpoint(
        self,
        committed_offsets: Dict[Any, int],
        dedup_operator: DeduplicationAndCleaningOperator,
        feature_extractor: FeatureExtractor,
        window_aggregator: Optional[EventTimeWindowAggregator] = None,
    ) -> Dict[str, Any]:
        """
        Atomically snapshot current operator state, seen `event_id`s, Welford baselines,
        event-time window buffers, and Kafka `committed_offsets` to durable storage.
        """
        start_ns = time.perf_counter_ns()
        self._sequence += 1
        checkpoint_id = f"chk-{self._sequence:06d}"

        win_agg = window_aggregator or feature_extractor.window_aggregator

        # Serialize customer baselines at full IEEE-754 precision
        baselines_dict: Dict[str, Dict[str, str]] = {}
        for cid, state in feature_extractor._baselines.items():
            h = state.to_redis_hash()
            h["prior_sum"] = repr(state.prior_sum)
            h["prior_mean"] = repr(state.prior_mean)
            h["prior_M2"] = repr(state.prior_M2)
            h["prior_std"] = repr(state.prior_std)
            baselines_dict[cid] = h

        # Serialize window events per customer
        window_events_dict: Dict[str, List[Dict[str, Any]]] = {}
        for cid, records in win_agg._customer_events.items():
            window_events_dict[cid] = [
                {
                    "event_id": r.event_id,
                    "customer_id": r.customer_id,
                    "merchant_id": r.merchant_id,
                    "amount": r.amount,
                    "event_time_epoch": r.event_time_epoch,
                    "channel": r.channel,
                    "country": r.country,
                }
                for r in records
            ]

        # Serialize 1m tumbling buckets
        buckets_1m_dict: Dict[str, Dict[str, Any]] = {}
        for b_key, b_state in win_agg._buckets_1m.items():
            buckets_1m_dict[str(b_key)] = {
                "bucket_start_epoch": b_state.bucket_start_epoch,
                "window_size": b_state.window_size,
                "window_seconds": b_state.window_seconds,
                "amounts": list(b_state.amounts),
                "customers": sorted(b_state.customers),
                "merchants": sorted(b_state.merchants),
                "event_ids": sorted(b_state.event_ids),
                "channel": b_state.channel,
                "region": b_state.region,
            }

        seen_union = sorted(
            set(dedup_operator._seen_event_ids.keys()) | set(feature_extractor._seen_event_ids)
        )
        normalized_offsets = {str(k): int(v) for k, v in committed_offsets.items()}

        snapshot: Dict[str, Any] = {
            "checkpoint_id": checkpoint_id,
            "sequence": self._sequence,
            "job_name": self.job_name,
            "epoch_ms": int(round(time.time() * 1000)),
            "committed_offsets": normalized_offsets,
            "offsets": normalized_offsets,
            "seen_event_ids": seen_union,
            "watermark_state": {
                "max_event_time_ms": dedup_operator.max_event_time_ms,
                "current_watermark_ms": dedup_operator.current_watermark_ms,
                "win_max_event_time_epoch": win_agg.max_event_time_epoch,
                "win_current_watermark_epoch": win_agg.current_watermark_epoch,
            },
            "customer_baselines": baselines_dict,
            "window_customer_events": window_events_dict,
            "window_buckets_1m": buckets_1m_dict,
        }

        payload_json = json.dumps(snapshot, indent=2, sort_keys=True)

        # Atomic write via temporary file -> rename
        chk_file = self.checkpoint_dir / f"checkpoint-{checkpoint_id}.json"
        tmp_file = self.checkpoint_dir / f".tmp-{checkpoint_id}.json"
        tmp_file.write_text(payload_json, encoding="utf-8")
        tmp_file.replace(chk_file)

        latest_file = self.checkpoint_dir / "latest.json"
        tmp_latest = self.checkpoint_dir / ".tmp-latest.json"
        tmp_latest.write_text(payload_json, encoding="utf-8")
        tmp_latest.replace(latest_file)

        if self.redis_client is not None:
            try:
                self.redis_client.set("flink:checkpoint:latest", payload_json)
            except Exception:
                pass

        duration_ms = round((time.perf_counter_ns() - start_ns) / 1_000_000.0, 3)
        self.last_checkpoint_id = checkpoint_id
        self.last_checkpoint_duration_ms = duration_ms
        snapshot["checkpoint_duration_ms"] = duration_ms

        if CHECKPOINT_DURATION_GAUGE is not None:
            try:
                CHECKPOINT_DURATION_GAUGE.labels(job_name=self.job_name).set(duration_ms)
            except Exception:
                pass

        return snapshot

    def load_latest_snapshot(self) -> Optional[Dict[str, Any]]:
        """Read the most recent committed checkpoint from disk or Redis."""
        latest_file = self.checkpoint_dir / "latest.json"
        if latest_file.exists():
            return json.loads(latest_file.read_text(encoding="utf-8"))

        if self.redis_client is not None:
            try:
                raw = self.redis_client.get("flink:checkpoint:latest")
                if raw:
                    if isinstance(raw, (bytes, bytearray)):
                        raw = raw.decode("utf-8")
                    return json.loads(raw)
            except Exception:
                pass

        chk_files = sorted(self.checkpoint_dir.glob("checkpoint-*.json"))
        if chk_files:
            return json.loads(chk_files[-1].read_text(encoding="utf-8"))
        return None

    def restore_latest_checkpoint(
        self,
        dedup_operator: DeduplicationAndCleaningOperator,
        feature_extractor: FeatureExtractor,
        window_aggregator: Optional[EventTimeWindowAggregator] = None,
    ) -> Dict[str, Any]:
        """
        Restore operator state from the latest committed checkpoint and return
        recovery telemetry (`checkpoint_id`, `committed_offsets`, `restore_outcome`,
        `recovery_time_ms`).
        """
        start_ns = time.perf_counter_ns()
        snapshot = self.load_latest_snapshot()
        if snapshot is None:
            telemetry = {
                "checkpoint_id": None,
                "committed_offsets": {},
                "offsets": {},
                "restore_outcome": "NO_CHECKPOINT_FOUND",
                "recovery_time_ms": 0.0,
                "restored_seen_events_count": 0,
                "restored_customers_count": 0,
            }
            self.last_recovery_telemetry = telemetry
            return telemetry

        win_agg = window_aggregator or feature_extractor.window_aggregator
        now_mono = time.monotonic()

        # 1. Restore deduplication seen_event_ids and watermarks
        dedup_operator._seen_event_ids.clear()
        seen_ids: List[str] = snapshot.get("seen_event_ids", [])
        for ev_id in seen_ids:
            dedup_operator._seen_event_ids[str(ev_id)] = now_mono + dedup_operator.dedup_ttl_sec
            if dedup_operator.redis_lookup is not None:
                dedup_operator.redis_lookup.mark_event_seen_nx(
                    str(ev_id), ttl_sec=dedup_operator.dedup_ttl_sec
                )

        wm_state = snapshot.get("watermark_state", {})
        dedup_operator.max_event_time_ms = wm_state.get("max_event_time_ms")
        dedup_operator.current_watermark_ms = wm_state.get("current_watermark_ms")

        # 2. Restore FeatureExtractor Welford baselines & seen IDs
        feature_extractor._seen_event_ids = set(str(x) for x in seen_ids)
        feature_extractor._baselines.clear()
        for cid, raw_hash in snapshot.get("customer_baselines", {}).items():
            baseline_state = CustomerBaselineState.from_redis_hash(cid, raw_hash)
            feature_extractor._save_baseline(baseline_state)

        # 3. Restore EventTimeWindowAggregator customer event buffers & 1m buckets
        win_agg.max_event_time_epoch = float(wm_state.get("win_max_event_time_epoch", 0.0) or 0.0)
        win_agg.current_watermark_epoch = float(
            wm_state.get("win_current_watermark_epoch", 0.0) or 0.0
        )
        win_agg._customer_events.clear()
        win_agg._customer_seen_ids.clear()
        for cid, rec_list in snapshot.get("window_customer_events", {}).items():
            restored_recs: List[WindowEventRecord] = []
            seen_set = set()
            for r in rec_list:
                rec_obj = WindowEventRecord(
                    event_id=str(r["event_id"]),
                    customer_id=str(r["customer_id"]),
                    merchant_id=str(r["merchant_id"]),
                    amount=float(r["amount"]),
                    event_time_epoch=float(r["event_time_epoch"]),
                    channel=str(r.get("channel", "online")),
                    country=str(r.get("country", "US")),
                )
                restored_recs.append(rec_obj)
                seen_set.add(rec_obj.event_id)
            win_agg._customer_events[cid] = restored_recs
            win_agg._customer_seen_ids[cid] = seen_set

        win_agg._buckets_1m.clear()
        for b_key_str, b_data in snapshot.get("window_buckets_1m", {}).items():
            b_epoch = int(b_key_str)
            b_state = TumblingBucketState(
                bucket_start_epoch=int(b_data["bucket_start_epoch"]),
                window_size=str(b_data.get("window_size", "1m")),
                window_seconds=int(b_data.get("window_seconds", 60)),
                amounts=[float(x) for x in b_data.get("amounts", [])],
                customers=set(b_data.get("customers", [])),
                merchants=set(b_data.get("merchants", [])),
                event_ids=set(b_data.get("event_ids", [])),
                channel=str(b_data.get("channel", "all")),
                region=str(b_data.get("region", "global")),
            )
            win_agg._buckets_1m[b_epoch] = b_state

        recovery_time_ms = round((time.perf_counter_ns() - start_ns) / 1_000_000.0, 3)
        self._sequence = max(self._sequence, int(snapshot.get("sequence", 1)))
        self.last_checkpoint_id = str(snapshot["checkpoint_id"])

        if RECOVERY_TIME_GAUGE is not None:
            try:
                RECOVERY_TIME_GAUGE.labels(job_name=self.job_name).set(recovery_time_ms)
            except Exception:
                pass

        committed_offsets = dict(snapshot.get("committed_offsets", {}))
        telemetry = {
            "checkpoint_id": str(snapshot["checkpoint_id"]),
            "committed_offsets": committed_offsets,
            "offsets": committed_offsets,
            "restore_outcome": "SUCCESS",
            "recovery_time_ms": recovery_time_ms,
            "restored_seen_events_count": len(seen_ids),
            "restored_customers_count": len(snapshot.get("customer_baselines", {})),
        }
        self.last_recovery_telemetry = telemetry
        return telemetry

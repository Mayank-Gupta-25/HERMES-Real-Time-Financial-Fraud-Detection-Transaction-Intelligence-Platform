"""
16-Feature Streaming Extractor with Leakage-Free Prior Baselines & Warm-Up Guard (`MOD-03`).

Per `Lab678_21.pdf` Sections 4 & 5 and Blueprint Section 10.D / Section 13 `MOD-03`:
  1. Reads `customer:{customer_id}:baseline` strictly from prior events (`t_i < t`)
     before updating state with the current event `e_t` (zero temporal leakage).
  2. Applies the warm-up guard: when `prior_count < 5`, sets `is_warmup = True`
     and `amount_z_score = 0.0` so new customers are never penalized by unstable
     initial variance.
  3. Computes all 16 canonical behavioral, velocity, geospatial, and temporal
     features (`F01`..`F16`) plus rule-support counters (`micro_txn_count_10m`,
     `structuring_txn_count_24h`, `unique_merchants_1h`, `is_warmup`).
  4. Updates Welford running mean/variance (`prior_count`, `prior_mean`, `prior_M2`,
     `prior_std`) and last-seen location/device/category state AFTER emitting the
     feature vector.
"""
from __future__ import annotations

import importlib.util
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_THIS_DIR = Path(__file__).resolve().parent
_APP_DIR = _THIS_DIR.parent


def _load_sibling_module(module_name: str, file_path: Path):
    import sys

    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {file_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


try:
    from .geo_distance import compute_geo_velocity
except Exception:
    _geo_mod = _load_sibling_module("geo_distance", _THIS_DIR / "geo_distance.py")
    compute_geo_velocity = _geo_mod.compute_geo_velocity

try:
    from ..jobs.aggregation_job import EventTimeWindowAggregator, parse_event_time_epoch
except Exception:
    _agg_mod = _load_sibling_module("aggregation_job", _APP_DIR / "jobs" / "aggregation_job.py")
    EventTimeWindowAggregator = _agg_mod.EventTimeWindowAggregator
    parse_event_time_epoch = _agg_mod.parse_event_time_epoch


FEATURE_COLUMNS: List[str] = [
    "txn_count_1m",           # F01
    "txn_count_5m",           # F02
    "txn_count_1h",           # F03
    "amount_sum_1h",          # F04
    "amount_mean_prior",      # F05
    "amount_std_prior",       # F06
    "amount_z_score",         # F07
    "time_since_last_sec",    # F08
    "geo_distance_km",        # F09
    "travel_speed_kmph",      # F10
    "is_international",       # F11
    "merchant_category_freq", # F12
    "hour_of_day",            # F13
    "day_of_week",            # F14
    "is_weekend",             # F15
    "device_change_flag",     # F16
]

WARMUP_MIN_EVENTS: int = 5
BASELINE_TTL_SECONDS: int = 604800  # 7 days


@dataclass
class CustomerBaselineState:
    customer_id: str
    prior_count: int = 0
    prior_sum: float = 0.0
    prior_mean: float = 0.0
    prior_M2: float = 0.0
    prior_std: float = 0.0
    last_event_time_epoch: Optional[float] = None
    last_lat: Optional[float] = None
    last_lon: Optional[float] = None
    last_country: Optional[str] = None
    last_device_type: Optional[str] = None
    category_counts: Dict[str, int] = field(default_factory=dict)
    country_counts: Dict[str, int] = field(default_factory=dict)
    recent_merchants: List[str] = field(default_factory=list)

    def to_redis_hash(self) -> Dict[str, str]:
        return {
            "prior_count": str(self.prior_count),
            "prior_sum": str(round(self.prior_sum, 6)),
            "prior_mean": str(round(self.prior_mean, 6)),
            "prior_M2": str(round(self.prior_M2, 6)),
            "prior_std": str(round(self.prior_std, 6)),
            "last_event_time_epoch": (
                "" if self.last_event_time_epoch is None else str(self.last_event_time_epoch)
            ),
            "last_lat": "" if self.last_lat is None else str(self.last_lat),
            "last_lon": "" if self.last_lon is None else str(self.last_lon),
            "last_country": self.last_country or "",
            "last_device_type": self.last_device_type or "",
            "category_counts_json": json.dumps(self.category_counts),
            "country_counts_json": json.dumps(self.country_counts),
            "recent_merchants_json": json.dumps(self.recent_merchants[-20:]),
        }

    @classmethod
    def from_redis_hash(cls, customer_id: str, raw: Dict[Any, Any]) -> "CustomerBaselineState":
        decoded: Dict[str, str] = {}
        for k, v in raw.items():
            key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            decoded[key_str] = val_str

        def _opt_float(key: str) -> Optional[float]:
            val = decoded.get(key, "")
            return float(val) if val not in ("", "None", None) else None

        return cls(
            customer_id=customer_id,
            prior_count=int(decoded.get("prior_count", 0) or 0),
            prior_sum=float(decoded.get("prior_sum", 0.0) or 0.0),
            prior_mean=float(decoded.get("prior_mean", 0.0) or 0.0),
            prior_M2=float(decoded.get("prior_M2", 0.0) or 0.0),
            prior_std=float(decoded.get("prior_std", 0.0) or 0.0),
            last_event_time_epoch=_opt_float("last_event_time_epoch"),
            last_lat=_opt_float("last_lat"),
            last_lon=_opt_float("last_lon"),
            last_country=decoded.get("last_country") or None,
            last_device_type=decoded.get("last_device_type") or None,
            category_counts=json.loads(decoded.get("category_counts_json") or "{}"),
            country_counts=json.loads(decoded.get("country_counts_json") or "{}"),
            recent_merchants=json.loads(decoded.get("recent_merchants_json") or "[]"),
        )


class FeatureExtractor:
    """
    Computes all 16 canonical HERMES features (`F01`..`F16`) for each incoming
    transaction while strictly enforcing prior-only Welford baselines (`t_i < t`)
    and the 5-event warm-up guard (`prior_count < 5` -> `is_warmup=True`,
    `amount_z_score=0.0`).
    """

    def __init__(
        self,
        window_aggregator: Optional[EventTimeWindowAggregator] = None,
        redis_client: Optional[Any] = None,
        warmup_min_events: int = WARMUP_MIN_EVENTS,
    ) -> None:
        self.window_aggregator = window_aggregator or EventTimeWindowAggregator()
        self.redis_client = redis_client
        self.warmup_min_events = int(warmup_min_events)
        self._baselines: Dict[str, CustomerBaselineState] = {}
        self._seen_event_ids: set[str] = set()

    def _get_baseline(self, customer_id: str) -> CustomerBaselineState:
        state = self._baselines.get(customer_id)
        if state is not None:
            return state
        if self.redis_client is not None:
            try:
                raw = self.redis_client.hgetall(f"customer:{customer_id}:baseline")
                if raw:
                    state = CustomerBaselineState.from_redis_hash(customer_id, raw)
                    self._baselines[customer_id] = state
                    return state
            except Exception:
                pass
        state = CustomerBaselineState(customer_id=customer_id)
        self._baselines[customer_id] = state
        return state

    def _save_baseline(self, state: CustomerBaselineState) -> None:
        self._baselines[state.customer_id] = state
        if self.redis_client is not None:
            try:
                key = f"customer:{state.customer_id}:baseline"
                self.redis_client.hset(key, mapping=state.to_redis_hash())
                self.redis_client.expire(key, BASELINE_TTL_SECONDS)
            except Exception:
                pass

    def extract_features(
        self,
        event: Dict[str, Any],
        window_metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Compute the 16 canonical features + auxiliary rule signals for `event`,
        strictly reading prior baseline state BEFORE updating Welford state.
        """
        customer_id = str(event["customer_id"])
        event_id = str(event.get("event_id") or event.get("transaction_id") or "")
        amount = float(event["amount"])
        category = str(event.get("category") or event.get("merchant_category") or "unknown")
        merchant_id = str(event.get("merchant_id") or "unknown")

        raw_time = event.get("event_time") or event.get("timestamp") or event.get("event_time_epoch")
        event_time_epoch = parse_event_time_epoch(raw_time)
        dt_utc = datetime.fromtimestamp(event_time_epoch, tz=timezone.utc)

        loc = event.get("location") or {}
        curr_lat = loc.get("latitude")
        curr_lon = loc.get("longitude")
        curr_country = str(loc.get("country") or event.get("country") or "US")

        raw_device = event.get("device") if "device" in event else event.get("device_info")
        device_type: Optional[str] = None
        if isinstance(raw_device, dict):
            device_type = raw_device.get("device_type")

        # ------------------------------------------------------------------
        # STEP 1: Read Prior Baseline State (t_i < t, zero temporal leakage)
        # ------------------------------------------------------------------
        state = self._get_baseline(customer_id)
        prior_count = state.prior_count
        amount_mean_prior = round(state.prior_mean, 4)
        amount_std_prior = round(state.prior_std, 4)

        # ------------------------------------------------------------------
        # STEP 2: Warm-Up Check (`prior_count < 5` -> `is_warmup = True`)
        # ------------------------------------------------------------------
        is_warmup = prior_count < self.warmup_min_events
        if is_warmup:
            amount_z_score = 0.0
        else:
            denom = max(state.prior_std, 1.0)
            amount_z_score = round((amount - state.prior_mean) / denom, 4)

        # ------------------------------------------------------------------
        # STEP 3: Compute Static 1m / 5m / 1h Window Counts & Geo/Temporal Features
        # ------------------------------------------------------------------
        win = window_metrics if window_metrics is not None else self.window_aggregator.process_event(event)

        if prior_count == 0 or state.last_event_time_epoch is None:
            time_since_last_sec = -1.0
            geo_distance_km = 0.0
            travel_speed_kmph = 0.0
        else:
            time_since_last_sec = round(max(0.0, event_time_epoch - state.last_event_time_epoch), 4)
            geo_distance_km, travel_speed_kmph = compute_geo_velocity(
                curr_lat=float(curr_lat) if curr_lat is not None else None,
                curr_lon=float(curr_lon) if curr_lon is not None else None,
                curr_time_epoch=event_time_epoch,
                prev_lat=state.last_lat,
                prev_lon=state.last_lon,
                prev_time_epoch=state.last_event_time_epoch,
            )

        home_country = event.get("customer_home_country") or state.last_country
        if "is_international" in event and event["is_international"] is not None:
            is_intl_flag = 1 if bool(event["is_international"]) else 0
        elif home_country:
            is_intl_flag = 1 if curr_country != str(home_country) else 0
        else:
            is_intl_flag = 0

        if prior_count > 0:
            merchant_category_freq = round(
                state.category_counts.get(category, 0) / float(max(prior_count, 1)),
                4,
            )
        else:
            merchant_category_freq = 0.0

        hour_of_day = int(dt_utc.hour)
        day_of_week = int(dt_utc.weekday())
        is_weekend = 1 if day_of_week in (5, 6) else 0

        if (
            prior_count > 0
            and device_type is not None
            and state.last_device_type is not None
            and device_type != state.last_device_type
        ):
            device_change_flag = 1
        else:
            device_change_flag = 0

        features: Dict[str, Any] = {
            # Canonical 16 features (F01..F16)
            "txn_count_1m": int(win["txn_count_1m"]),
            "txn_count_5m": int(win["txn_count_5m"]),
            "txn_count_1h": int(win["txn_count_1h"]),
            "amount_sum_1h": float(win["amount_sum_1h"]),
            "amount_mean_prior": float(amount_mean_prior),
            "amount_std_prior": float(amount_std_prior),
            "amount_z_score": float(amount_z_score),
            "time_since_last_sec": float(time_since_last_sec),
            "geo_distance_km": float(geo_distance_km),
            "travel_speed_kmph": float(travel_speed_kmph),
            "is_international": int(is_intl_flag),
            "merchant_category_freq": float(merchant_category_freq),
            "hour_of_day": int(hour_of_day),
            "day_of_week": int(day_of_week),
            "is_weekend": int(is_weekend),
            "device_change_flag": int(device_change_flag),
            # Auxiliary rule & provenance fields
            "is_warmup": bool(is_warmup),
            "prior_count": int(prior_count),
            "micro_txn_count_10m": int(win.get("micro_txn_count_10m", 0)),
            "structuring_txn_count_24h": int(win.get("structuring_txn_count_24h", 0)),
            "unique_merchants_1h": int(win.get("unique_merchants_1h", 1)),
            "amount_max_1h": float(win.get("amount_max_1h", amount)),
        }

        # ------------------------------------------------------------------
        # STEP 4: Update Welford Running State AFTER Emitting Features
        # ------------------------------------------------------------------
        if not event_id or event_id not in self._seen_event_ids:
            if event_id:
                self._seen_event_ids.add(event_id)
            n_new = state.prior_count + 1
            delta = amount - state.prior_mean
            mean_new = state.prior_mean + (delta / float(n_new))
            delta2 = amount - mean_new
            m2_new = state.prior_M2 + (delta * delta2)
            std_new = math.sqrt(max(0.0, m2_new / float(n_new - 1))) if n_new > 1 else 0.0

            state.prior_count = n_new
            state.prior_sum += amount
            state.prior_mean = mean_new
            state.prior_M2 = m2_new
            state.prior_std = std_new
            if state.last_event_time_epoch is None or event_time_epoch >= state.last_event_time_epoch:
                state.last_event_time_epoch = event_time_epoch
                if curr_lat is not None and curr_lon is not None:
                    state.last_lat = float(curr_lat)
                    state.last_lon = float(curr_lon)
                state.last_country = curr_country
                if device_type is not None:
                    state.last_device_type = device_type

            state.category_counts[category] = state.category_counts.get(category, 0) + 1
            state.country_counts[curr_country] = state.country_counts.get(curr_country, 0) + 1
            state.recent_merchants.append(merchant_id)
            if len(state.recent_merchants) > 25:
                state.recent_merchants = state.recent_merchants[-25:]

            self._save_baseline(state)

        return features

    def to_feature_vector(self, features: Dict[str, Any]) -> List[float]:
        """Return the ordered 16-element float vector matching `FEATURE_COLUMNS`."""
        return [float(features[col]) for col in FEATURE_COLUMNS]

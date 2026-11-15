from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from typing import Any
import uuid

try:
    import redis  # type: ignore[import-not-found]
except ImportError:  # Clean fallback when redis package is not installed in host Python
    redis = None  # type: ignore[assignment]

from ..config import settings


EARTH_RADIUS_KM = 6371.0088


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in kilometers between two GPS coordinates."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    a = min(1.0, max(0.0, a))
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return EARTH_RADIUS_KM * c


def _parse_event_epoch(raw_ts: Any) -> tuple[float, datetime]:
    if isinstance(raw_ts, (int, float)):
        epoch = float(raw_ts)
        return epoch, datetime.fromtimestamp(epoch, tz=timezone.utc)
    if isinstance(raw_ts, str) and raw_ts.strip():
        try:
            normalized = raw_ts.strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.timestamp(), dt
        except ValueError:
            pass
    now_dt = datetime.now(timezone.utc)
    return now_dt.timestamp(), now_dt


class VelocityChecker:
    """
    Sliding-window velocity checker and prior-only Welford baseline tracker.
    Uses Redis ZSETs + HASHes (`velocity:{customer_id}`, `velocity:micro:{customer_id}`,
    `velocity:structuring:{customer_id}`, `customer:{customer_id}:baseline`) when a live
    Redis instance is connected, and transparently falls back to deterministic in-memory
    sorted velocity state for unit testing or offline scoring.

    Strictly enforces the Section 10.D Temporal Leakage Prevention Rule:
      1. Read prior customer baseline state strictly before updating.
      2. Apply warm-up guard (`prior_count < 5` -> `is_warmup = True`, `amount_z_score = 0.0`).
      3. Emit feature vector.
      4. Update Welford state (`prior_count`, `prior_mean`, `prior_M2`, `prior_std`) for future events.
    """

    def __init__(
        self,
        redis_client: Any | None = None,
        warmup_min_events: int | None = None,
        use_redis: bool = False,
    ) -> None:
        self.warmup_min_events = (
            warmup_min_events if warmup_min_events is not None else settings.warmup_min_events
        )
        self._redis: Any | None = redis_client
        if self._redis is None and use_redis and redis is not None:
            try:
                client = redis.Redis(
                    host=settings.redis_host,
                    port=settings.redis_port,
                    db=settings.redis_db,
                    decode_responses=True,
                    socket_connect_timeout=0.25,
                    socket_timeout=0.25,
                )
                client.ping()
                self._redis = client
            except Exception:
                self._redis = None

        # In-memory sorted state fallback: customer_id -> list of (epoch_sec, event_id, amount)
        self._mem_velocity: dict[str, list[tuple[float, str, float]]] = {}
        self._mem_micro: dict[str, list[tuple[float, str, float]]] = {}
        self._mem_structuring: dict[str, list[tuple[float, str, float]]] = {}
        self._mem_baseline: dict[str, dict[str, Any]] = {}

    def reset_state(self) -> None:
        """Clear in-memory velocity and baseline state (useful between unit tests)."""
        self._mem_velocity.clear()
        self._mem_micro.clear()
        self._mem_structuring.clear()
        self._mem_baseline.clear()

    def _get_prior_baseline(self, customer_id: str) -> dict[str, Any]:
        if self._redis is not None:
            try:
                raw = self._redis.hgetall(f"customer:{customer_id}:baseline")
                if raw:
                    return {
                        "prior_count": int(raw.get("prior_count", 0)),
                        "prior_sum": float(raw.get("prior_sum", 0.0)),
                        "prior_M2": float(raw.get("prior_M2", 0.0)),
                        "prior_mean": float(raw.get("prior_mean", 0.0)),
                        "prior_std": float(raw.get("prior_std", 0.0)),
                        "last_event_time_epoch": (
                            float(raw["last_event_time_epoch"])
                            if raw.get("last_event_time_epoch") not in (None, "", "None")
                            else None
                        ),
                        "last_lat": (
                            float(raw["last_lat"])
                            if raw.get("last_lat") not in (None, "", "None")
                            else None
                        ),
                        "last_lon": (
                            float(raw["last_lon"])
                            if raw.get("last_lon") not in (None, "", "None")
                            else None
                        ),
                        "last_country": raw.get("last_country") or None,
                        "last_device_type": raw.get("last_device_type") or None,
                        "category_counts": json.loads(raw.get("category_counts_json", "{}")),
                    }
            except Exception:
                pass

        state = self._mem_baseline.get(customer_id)
        if state is None:
            return {
                "prior_count": 0,
                "prior_sum": 0.0,
                "prior_M2": 0.0,
                "prior_mean": 0.0,
                "prior_std": 0.0,
                "last_event_time_epoch": None,
                "last_lat": None,
                "last_lon": None,
                "last_country": None,
                "last_device_type": None,
                "category_counts": {},
            }
        return {
            **state,
            "category_counts": dict(state.get("category_counts", {})),
        }

    def _save_updated_baseline(self, customer_id: str, updated: dict[str, Any]) -> None:
        self._mem_baseline[customer_id] = updated
        if self._redis is not None:
            try:
                key = f"customer:{customer_id}:baseline"
                mapping = {
                    "prior_count": str(updated["prior_count"]),
                    "prior_sum": str(updated["prior_sum"]),
                    "prior_M2": str(updated["prior_M2"]),
                    "prior_mean": str(updated["prior_mean"]),
                    "prior_std": str(updated["prior_std"]),
                    "last_event_time_epoch": (
                        str(updated["last_event_time_epoch"])
                        if updated["last_event_time_epoch"] is not None
                        else ""
                    ),
                    "last_lat": str(updated["last_lat"]) if updated["last_lat"] is not None else "",
                    "last_lon": str(updated["last_lon"]) if updated["last_lon"] is not None else "",
                    "last_country": updated["last_country"] or "",
                    "last_device_type": updated["last_device_type"] or "",
                    "category_counts_json": json.dumps(updated.get("category_counts", {})),
                }
                self._redis.hset(key, mapping=mapping)
                self._redis.expire(key, 604800)  # 7 days TTL
            except Exception:
                pass

    def _record_and_query_windows(
        self,
        customer_id: str,
        event_id: str,
        event_epoch: float,
        amount: float,
    ) -> dict[str, float | int]:
        # Always maintain in-memory sorted window state for exact amount_sum_1h & fallback
        vel_list = self._mem_velocity.setdefault(customer_id, [])
        # Prune entries older than 1h + 60s relative to event_epoch
        cutoff_1h = event_epoch - 3600.0
        vel_list[:] = [item for item in vel_list if item[0] >= cutoff_1h - 60.0]
        if not any(item[1] == event_id for item in vel_list):
            vel_list.append((event_epoch, event_id, amount))
            vel_list.sort(key=lambda x: x[0])

        micro_list = self._mem_micro.setdefault(customer_id, [])
        cutoff_10m = event_epoch - 600.0
        micro_list[:] = [item for item in micro_list if item[0] >= cutoff_10m - 60.0]
        if 0.0 < amount <= 5.0 and not any(item[1] == event_id for item in micro_list):
            micro_list.append((event_epoch, event_id, amount))
            micro_list.sort(key=lambda x: x[0])

        struct_list = self._mem_structuring.setdefault(customer_id, [])
        cutoff_24h = event_epoch - 86400.0
        struct_list[:] = [item for item in struct_list if item[0] >= cutoff_24h]
        if 8000.0 <= amount < 10000.0 and not any(item[1] == event_id for item in struct_list):
            struct_list.append((event_epoch, event_id, amount))
            struct_list.sort(key=lambda x: x[0])

        txn_count_1m = sum(
            1 for ts, _, _ in vel_list if (event_epoch - 60.0) <= ts <= event_epoch
        )
        txn_count_5m = sum(
            1 for ts, _, _ in vel_list if (event_epoch - 300.0) <= ts <= event_epoch
        )
        txn_count_1h = sum(
            1 for ts, _, _ in vel_list if (event_epoch - 3600.0) <= ts <= event_epoch
        )
        amount_sum_1h = sum(
            amt for ts, _, amt in vel_list if (event_epoch - 3600.0) <= ts <= event_epoch
        )
        micro_txn_count_10m = sum(
            1 for ts, _, _ in micro_list if (event_epoch - 600.0) <= ts <= event_epoch
        )
        structuring_txn_count_24h = sum(
            1 for ts, _, _ in struct_list if (event_epoch - 86400.0) <= ts <= event_epoch
        )

        if self._redis is not None:
            try:
                vel_key = f"velocity:{customer_id}"
                self._redis.zadd(vel_key, {event_id: event_epoch})
                self._redis.zremrangebyscore(vel_key, "-inf", event_epoch - 3660.0)
                self._redis.expire(vel_key, 3660)
                txn_count_1m = int(self._redis.zcount(vel_key, event_epoch - 60.0, event_epoch))
                txn_count_5m = int(self._redis.zcount(vel_key, event_epoch - 300.0, event_epoch))
                txn_count_1h = int(self._redis.zcount(vel_key, event_epoch - 3600.0, event_epoch))

                micro_key = f"velocity:micro:{customer_id}"
                if 0.0 < amount <= 5.0:
                    self._redis.zadd(micro_key, {event_id: event_epoch})
                    self._redis.expire(micro_key, 660)
                self._redis.zremrangebyscore(micro_key, "-inf", event_epoch - 660.0)
                micro_txn_count_10m = int(
                    self._redis.zcount(micro_key, event_epoch - 600.0, event_epoch)
                )

                struct_key = f"velocity:structuring:{customer_id}"
                if 8000.0 <= amount < 10000.0:
                    self._redis.zadd(struct_key, {event_id: event_epoch})
                    self._redis.expire(struct_key, 86400)
                self._redis.zremrangebyscore(struct_key, "-inf", event_epoch - 86400.0)
                structuring_txn_count_24h = int(
                    self._redis.zcount(struct_key, event_epoch - 86400.0, event_epoch)
                )
            except Exception:
                pass

        return {
            "txn_count_1m": max(1, txn_count_1m),
            "txn_count_5m": max(1, txn_count_5m),
            "txn_count_1h": max(1, txn_count_1h),
            "amount_sum_1h": round(amount_sum_1h, 2),
            "micro_txn_count_10m": micro_txn_count_10m,
            "structuring_txn_count_24h": structuring_txn_count_24h,
        }

    def compute_features(
        self,
        event: dict[str, Any],
        provided_features: dict[str, Any] | None = None,
        update_state: bool = True,
    ) -> dict[str, Any]:
        """
        Compute or enrich the 16 canonical behavioral features + auxiliary velocity counters
        strictly following the Section 10.D Temporal Leakage Prevention Rule.
        """
        customer_id = str(event.get("customer_id") or "unknown-customer")
        event_id = str(
            event.get("event_id") or event.get("transaction_id") or uuid.uuid4()
        )
        amount = float(event.get("amount", 0.0))
        category = str(
            event.get("category") or event.get("merchant_category") or "retail"
        ).strip().lower()

        raw_ts = event.get("event_time") or event.get("timestamp")
        event_epoch, event_dt = _parse_event_epoch(raw_ts)

        loc = event.get("location") or {}
        lat = float(loc.get("latitude", event.get("latitude", 0.0)) or 0.0)
        lon = float(loc.get("longitude", event.get("longitude", 0.0)) or 0.0)
        country = str(loc.get("country", event.get("country", "US")) or "US").upper()

        dev = event.get("device") or event.get("device_info") or {}
        device_type = str(dev.get("device_type", event.get("device_type", "mobile")) or "mobile")

        # Step 1: Read prior customer baseline state ONLY (strictly before updating)
        prior = self._get_prior_baseline(customer_id)
        prior_count = int(prior["prior_count"])
        prior_mean = float(prior["prior_mean"])
        prior_std = float(prior["prior_std"])
        prior_sum = float(prior["prior_sum"])
        prior_m2 = float(prior["prior_M2"])

        # Step 2: Warm-up check (`prior_count < 5` -> `is_warmup = True`, `amount_z_score = 0.0`)
        is_warmup = prior_count < self.warmup_min_events
        if is_warmup:
            amount_z_score = 0.0
        else:
            denom = max(prior_std, 1.0)
            amount_z_score = round((amount - prior_mean) / denom, 4)

        last_epoch = prior["last_event_time_epoch"]
        if last_epoch is None or prior_count == 0:
            time_since_last_sec = -1.0
        else:
            time_since_last_sec = round(max(0.0, event_epoch - float(last_epoch)), 3)

        last_lat = prior["last_lat"]
        last_lon = prior["last_lon"]
        if last_lat is not None and last_lon is not None and prior_count > 0:
            geo_distance_km = round(
                haversine_distance_km(float(last_lat), float(last_lon), lat, lon), 4
            )
            if time_since_last_sec > 0.0:
                hours = max(time_since_last_sec / 3600.0, 1.0 / 3600.0)
                travel_speed_kmph = round(geo_distance_km / hours, 4)
            elif geo_distance_km > 1.0:
                travel_speed_kmph = round(geo_distance_km * 3600.0, 4)
            else:
                travel_speed_kmph = 0.0
        else:
            geo_distance_km = 0.0
            travel_speed_kmph = 0.0

        home_country = str(
            event.get("customer_home_country") or prior.get("last_country") or country
        ).upper()
        if "is_international" in event and event["is_international"] is not None:
            is_international = 1 if bool(event["is_international"]) else 0
        else:
            is_international = 1 if country != home_country else 0

        cat_counts: dict[str, int] = dict(prior.get("category_counts", {}))
        if prior_count > 0:
            merchant_category_freq = round(cat_counts.get(category, 0) / float(prior_count), 4)
        else:
            merchant_category_freq = 0.0

        hour_of_day = event_dt.hour
        day_of_week = event_dt.weekday()
        is_weekend = 1 if day_of_week in (5, 6) else 0

        last_device_type = prior.get("last_device_type")
        device_change_flag = (
            1 if (prior_count > 0 and last_device_type and device_type != last_device_type) else 0
        )

        # Compute sliding window counts
        win = self._record_and_query_windows(customer_id, event_id, event_epoch, amount)

        computed: dict[str, Any] = {
            "txn_count_1m": int(win["txn_count_1m"]),
            "txn_count_5m": int(win["txn_count_5m"]),
            "txn_count_1h": int(win["txn_count_1h"]),
            "amount_sum_1h": float(win["amount_sum_1h"]),
            "amount_mean_prior": round(prior_mean, 4),
            "amount_std_prior": round(prior_std, 4),
            "amount_z_score": float(amount_z_score),
            "time_since_last_sec": float(time_since_last_sec),
            "geo_distance_km": float(geo_distance_km),
            "travel_speed_kmph": float(travel_speed_kmph),
            "is_international": int(is_international),
            "merchant_category_freq": float(merchant_category_freq),
            "hour_of_day": int(hour_of_day),
            "day_of_week": int(day_of_week),
            "is_weekend": int(is_weekend),
            "device_change_flag": int(device_change_flag),
            "micro_txn_count_10m": int(win["micro_txn_count_10m"]),
            "structuring_txn_count_24h": int(win["structuring_txn_count_24h"]),
            "prior_count": int(prior_count),
            "is_warmup": bool(is_warmup),
        }

        # Step 4: Update Welford running baseline state AFTER computing features for current event
        if update_state:
            new_count = prior_count + 1
            new_sum = prior_sum + amount
            delta = amount - prior_mean
            new_mean = prior_mean + (delta / float(new_count))
            delta2 = amount - new_mean
            new_m2 = prior_m2 + delta * delta2
            new_std = math.sqrt(new_m2 / float(new_count - 1)) if new_count > 1 else 0.0
            cat_counts[category] = cat_counts.get(category, 0) + 1

            updated_state = {
                "prior_count": new_count,
                "prior_sum": round(new_sum, 4),
                "prior_M2": round(new_m2, 6),
                "prior_mean": round(new_mean, 4),
                "prior_std": round(new_std, 4),
                "last_event_time_epoch": event_epoch,
                "last_lat": lat,
                "last_lon": lon,
                "last_country": country,
                "last_device_type": device_type,
                "category_counts": cat_counts,
            }
            self._save_updated_baseline(customer_id, updated_state)

        # Merge provided features if caller supplied pre-extracted Flink features
        if provided_features:
            merged = dict(computed)
            for k, v in provided_features.items():
                if v is not None:
                    merged[k] = v
            # Enforce warm-up invariant if caller explicitly set prior_count < warmup_min_events
            if "prior_count" in provided_features and "is_warmup" not in provided_features:
                if int(provided_features["prior_count"]) < self.warmup_min_events:
                    merged["is_warmup"] = True
                    merged["amount_z_score"] = 0.0
            elif merged.get("is_warmup") is True and "amount_z_score" not in provided_features:
                merged["amount_z_score"] = 0.0
            return merged

        return computed

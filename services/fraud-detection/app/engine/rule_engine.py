from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import time
from typing import Any

import yaml

from ..config import settings
from .rules import RuleDefinition, RuleEvaluationResult, TriggeredRuleDetail


def _extract_hour_utc(event: dict[str, Any], features: dict[str, Any]) -> int:
    if "hour_of_day" in features and features["hour_of_day"] is not None:
        try:
            return int(features["hour_of_day"]) % 24
        except (ValueError, TypeError):
            pass

    raw_ts = event.get("event_time") or event.get("timestamp")
    if isinstance(raw_ts, str) and raw_ts:
        try:
            normalized = raw_ts.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc)
            return dt.hour
        except ValueError:
            pass
    return 12


def _to_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _to_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _to_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "y")
    return default


class RuleEngine:
    """
    Deterministic, hot-reloadable YAML fraud rule engine evaluating all 10 rules in < 2ms.
    Conforms to Section 10.B and Section 11.2 (`STEP MAYANK-MOD04-01`).
    """

    def __init__(self, rules_path: str | Path | None = None) -> None:
        self.rules_path = Path(rules_path or settings.rules_path)
        self.rule_version: str = settings.rule_version
        self.rules: list[RuleDefinition] = []
        self._rules_by_name: dict[str, RuleDefinition] = {}
        self._last_mtime: float = 0.0
        self.reload_rules(force=True)

    def reload_rules(self, force: bool = False) -> dict[str, Any]:
        """Load or hot-reload rules from `default_rules.yaml` if file mtime changed or forced."""
        if not self.rules_path.exists():
            # Fallback resolution relative to this file
            alt_path = Path(__file__).resolve().parent.parent / "rules" / "default_rules.yaml"
            if alt_path.exists():
                self.rules_path = alt_path

        if not self.rules_path.exists():
            raise FileNotFoundError(f"Rules file not found at {self.rules_path}")

        current_mtime = os.path.getmtime(self.rules_path)
        if not force and self.rules and current_mtime <= self._last_mtime:
            return {
                "reloaded": False,
                "rule_version": self.rule_version,
                "rule_count": len(self.rules),
            }

        with open(self.rules_path, "r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}

        self.rule_version = str(payload.get("rule_version", settings.rule_version))
        raw_rules = payload.get("rules", [])
        loaded_rules: list[RuleDefinition] = []
        for item in raw_rules:
            loaded_rules.append(RuleDefinition(**item))

        self.rules = loaded_rules
        self._rules_by_name = {r.name: r for r in loaded_rules}
        self._last_mtime = current_mtime
        return {
            "reloaded": True,
            "rule_version": self.rule_version,
            "rule_count": len(self.rules),
        }

    def get_rules_metadata(self) -> dict[str, Any]:
        """Return active ruleset metadata for `GET /rules`."""
        return {
            "rule_version": self.rule_version,
            "rules_path": str(self.rules_path),
            "total_rules": len(self.rules),
            "enabled_rules": sum(1 for r in self.rules if r.enabled),
            "rules": [r.model_dump() for r in self.rules],
        }

    def evaluate(
        self,
        event: dict[str, Any],
        features: dict[str, Any] | None = None,
        auto_reload: bool = False,
    ) -> RuleEvaluationResult:
        """
        Evaluate all 10 deterministic fraud rules in < 2ms.
        Enforces warm-up guard (`not is_warmup`) on `AMOUNT_SPIKE` (`Lab678_21.pdf` Sec. 4).
        """
        start_ns = time.perf_counter_ns()
        if auto_reload:
            self.reload_rules(force=False)

        feat = features or {}

        amount = _to_float(event.get("amount", feat.get("amount", 0.0)), 0.0)
        category = str(
            event.get("category")
            or event.get("merchant_category")
            or feat.get("category")
            or "unknown"
        ).strip().lower()

        txn_count_1m = _to_int(feat.get("txn_count_1m", event.get("txn_count_1m", 1)), 1)
        txn_count_5m = _to_int(feat.get("txn_count_5m", event.get("txn_count_5m", 1)), 1)
        txn_count_1h = _to_int(feat.get("txn_count_1h", event.get("txn_count_1h", 1)), 1)
        amount_sum_1h = _to_float(feat.get("amount_sum_1h", event.get("amount_sum_1h", amount)), amount)

        is_warmup = _to_bool(feat.get("is_warmup", event.get("is_warmup", False)), False)
        if "prior_count" in feat and "is_warmup" not in feat:
            is_warmup = _to_int(feat.get("prior_count"), 0) < settings.warmup_min_events

        amount_z_score = _to_float(feat.get("amount_z_score", event.get("amount_z_score", 0.0)), 0.0)
        travel_speed_kmph = _to_float(
            feat.get("travel_speed_kmph", event.get("travel_speed_kmph", 0.0)), 0.0
        )
        geo_distance_km = _to_float(
            feat.get("geo_distance_km", event.get("geo_distance_km", 0.0)), 0.0
        )
        micro_txn_count_10m = _to_int(
            feat.get("micro_txn_count_10m", event.get("micro_txn_count_10m", 0)), 0
        )
        structuring_txn_count_24h = _to_int(
            feat.get("structuring_txn_count_24h", event.get("structuring_txn_count_24h", 0)), 0
        )
        time_since_last_sec = _to_float(
            feat.get("time_since_last_sec", event.get("time_since_last_sec", -1.0)), -1.0
        )
        device_change_flag = _to_int(
            feat.get("device_change_flag", event.get("device_change_flag", 0)), 0
        )

        is_international_val = feat.get("is_international", event.get("is_international", 0))
        is_international = 1 if _to_bool(is_international_val, False) else 0

        merchant_is_high_risk = _to_bool(
            event.get("merchant_is_high_risk", feat.get("merchant_is_high_risk", False)), False
        ) or _to_bool(
            event.get("merchant_blacklisted", feat.get("merchant_blacklisted", False)), False
        )

        hour_of_day = _extract_hour_utc(event, feat)

        triggered_names: list[str] = []
        reasons: list[str] = []
        details: list[TriggeredRuleDetail] = []
        total_weight = 0.0
        has_critical = False
        has_block = False

        for rule in self.rules:
            if not rule.enabled:
                continue

            cond = rule.conditions
            matched = False
            reason_msg = ""

            if rule.name == "VELOCITY_EXCEEDED":
                limit_1h = _to_int(cond.get("txn_count_1h_gt", 10), 10)
                limit_5m = _to_int(cond.get("txn_count_5m_gte", 5), 5)
                if txn_count_1h > limit_1h or txn_count_5m >= limit_5m:
                    matched = True
                    reason_msg = (
                        f"VELOCITY_EXCEEDED: txn_count_1h={txn_count_1h} (>{limit_1h}) "
                        f"or txn_count_5m={txn_count_5m} (>={limit_5m})"
                    )

            elif rule.name == "AMOUNT_SPIKE":
                z_thresh = _to_float(cond.get("amount_z_score_gt", 3.0), 3.0)
                amt_thresh = _to_float(cond.get("amount_gt", 500.0), 500.0)
                if (not is_warmup) and amount_z_score > z_thresh and amount > amt_thresh:
                    matched = True
                    reason_msg = (
                        f"AMOUNT_SPIKE: amount=${amount:.2f} (>${amt_thresh:.2f}) with "
                        f"z_score={amount_z_score:.2f} (>{z_thresh:.1f})"
                    )

            elif rule.name == "GEO_IMPOSSIBILITY":
                speed_thresh = _to_float(cond.get("travel_speed_kmph_gt", 800.0), 800.0)
                dist_thresh = _to_float(cond.get("geo_distance_km_gt", 100.0), 100.0)
                if travel_speed_kmph > speed_thresh and geo_distance_km > dist_thresh:
                    matched = True
                    reason_msg = (
                        f"GEO_IMPOSSIBILITY: Travel speed {travel_speed_kmph:.1f} km/h "
                        f"exceeds {speed_thresh:.1f} km/h over {geo_distance_km:.1f} km"
                    )

            elif rule.name == "CARD_TESTING":
                micro_gte = _to_int(cond.get("micro_txn_count_10m_gte", 3), 3)
                followup_amt = _to_float(cond.get("followup_amount_gte", 100.0), 100.0)
                burst_5m = _to_int(cond.get("burst_txn_count_5m_gte", 4), 4)
                micro_amt_max = _to_float(cond.get("micro_amount_lte", 5.0), 5.0)
                if (micro_txn_count_10m >= micro_gte and amount >= followup_amt) or (
                    (txn_count_5m >= burst_5m or micro_txn_count_10m >= burst_5m)
                    and 0.0 < amount <= micro_amt_max
                ):
                    matched = True
                    reason_msg = (
                        f"CARD_TESTING: micro_txn_count_10m={micro_txn_count_10m}, "
                        f"txn_count_5m={txn_count_5m}, amount=${amount:.2f}"
                    )

            elif rule.name == "HIGH_RISK_MERCHANT":
                hr_categories = [
                    str(c).lower()
                    for c in cond.get(
                        "high_risk_categories",
                        ["gambling", "crypto", "money_transfer", "jewelry"],
                    )
                ]
                cat_amt_gt = _to_float(cond.get("category_amount_gt", 2000.0), 2000.0)
                if merchant_is_high_risk or (category in hr_categories and amount > cat_amt_gt):
                    matched = True
                    reason_msg = (
                        f"HIGH_RISK_MERCHANT: merchant_is_high_risk={merchant_is_high_risk}, "
                        f"category='{category}', amount=${amount:.2f}"
                    )

            elif rule.name == "UNUSUAL_HOUR":
                hours = [int(h) for h in cond.get("unusual_hours", [1, 2, 3, 4])]
                amt_gt = _to_float(cond.get("amount_gt", 1000.0), 1000.0)
                if hour_of_day in hours and amount > amt_gt:
                    matched = True
                    reason_msg = (
                        f"UNUSUAL_HOUR: hour_of_day={hour_of_day:02d}:00 UTC with "
                        f"amount=${amount:.2f} (>${amt_gt:.2f})"
                    )

            elif rule.name == "NEW_DEVICE_HIGH_AMOUNT":
                amt_gt = _to_float(cond.get("amount_gt", 1000.0), 1000.0)
                if device_change_flag == 1 and amount > amt_gt:
                    matched = True
                    reason_msg = (
                        f"NEW_DEVICE_HIGH_AMOUNT: device_change_flag=1 with "
                        f"amount=${amount:.2f} (>${amt_gt:.2f})"
                    )

            elif rule.name == "RAPID_SUCCESSION":
                max_gap = _to_float(cond.get("time_since_last_sec_lt", 30.0), 30.0)
                min_1m = _to_int(cond.get("txn_count_1m_gte", 2), 2)
                if 0.0 <= time_since_last_sec < max_gap and txn_count_1m >= min_1m:
                    matched = True
                    reason_msg = (
                        f"RAPID_SUCCESSION: time_since_last_sec={time_since_last_sec:.1f}s "
                        f"(<{max_gap:.1f}s) with txn_count_1m={txn_count_1m}"
                    )

            elif rule.name == "INTERNATIONAL_HIGH_AMOUNT":
                amt_gt = _to_float(cond.get("amount_gt", 2500.0), 2500.0)
                if is_international == 1 and amount > amt_gt:
                    matched = True
                    reason_msg = (
                        f"INTERNATIONAL_HIGH_AMOUNT: International transaction "
                        f"${amount:.2f} > ${amt_gt:.2f}"
                    )

            elif rule.name == "STRUCTURING":
                low_bound = _to_float(cond.get("amount_gte", 8000.0), 8000.0)
                high_bound = _to_float(cond.get("amount_lt", 10000.0), 10000.0)
                struct_cnt = _to_int(cond.get("structuring_txn_count_24h_gte", 2), 2)
                sum_1h_gte = _to_float(cond.get("amount_sum_1h_gte", 16000.0), 16000.0)
                if low_bound <= amount < high_bound and (
                    structuring_txn_count_24h >= struct_cnt or amount_sum_1h >= sum_1h_gte
                ):
                    matched = True
                    reason_msg = (
                        f"STRUCTURING: amount=${amount:.2f} in [${low_bound:.0f}, ${high_bound:.0f}) "
                        f"with structuring_txn_count_24h={structuring_txn_count_24h}, "
                        f"amount_sum_1h=${amount_sum_1h:.2f}"
                    )

            if matched:
                triggered_names.append(rule.name)
                reasons.append(reason_msg)
                details.append(
                    TriggeredRuleDetail(
                        rule_id=rule.id,
                        rule_name=rule.name,
                        weight=rule.weight,
                        severity=rule.severity,
                        action=rule.action,
                        reason=reason_msg,
                    )
                )
                total_weight += rule.weight
                if rule.severity == "critical":
                    has_critical = True
                if rule.action == "block":
                    has_block = True

        # Ensure a triggered critical rule yields rule_score >= 0.65
        if has_critical:
            total_weight = max(total_weight, 0.65)
        rule_score = min(1.0, max(0.0, round(total_weight, 4)))
        elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0

        return RuleEvaluationResult(
            rule_score=rule_score,
            triggered_rules=triggered_names,
            reasons=reasons,
            rule_details=details,
            rule_version=self.rule_version,
            has_critical_rule=has_critical,
            has_block_action=has_block,
            evaluation_time_ms=round(elapsed_ms, 4),
        )

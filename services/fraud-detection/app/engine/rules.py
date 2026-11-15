from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class RuleDefinition(BaseModel):
    """Single deterministic fraud rule loaded from `default_rules.yaml`."""

    id: str
    name: str
    enabled: bool = True
    weight: float = Field(ge=0.0, le=1.0)
    severity: str = "medium"  # low | medium | high | critical
    action: str = "flag"  # approve | flag | block
    description: str = ""
    conditions: dict[str, Any] = Field(default_factory=dict)


class TriggeredRuleDetail(BaseModel):
    """Detailed explanation for a single triggered rule."""

    rule_id: str
    rule_name: str
    weight: float
    severity: str
    action: str
    reason: str


class RuleEvaluationResult(BaseModel):
    """Deterministic evaluation output from `RuleEngine.evaluate`."""

    rule_score: float = Field(ge=0.0, le=1.0)
    triggered_rules: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    rule_details: list[TriggeredRuleDetail] = Field(default_factory=list)
    rule_version: str = "rules-v1.0"
    has_critical_rule: bool = False
    has_block_action: bool = False
    evaluation_time_ms: float = 0.0

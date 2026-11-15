"""Fraud detection engine package: YAML rules, Redis/in-memory velocity, and hybrid scorer."""

from .rules import RuleDefinition, RuleEvaluationResult, TriggeredRuleDetail
from .rule_engine import RuleEngine
from .velocity_checker import VelocityChecker, haversine_distance_km
from .scorer import StatisticalScorer, HybridFraudScorer

__all__ = [
    "RuleDefinition",
    "RuleEvaluationResult",
    "TriggeredRuleDetail",
    "RuleEngine",
    "VelocityChecker",
    "haversine_distance_km",
    "StatisticalScorer",
    "HybridFraudScorer",
]

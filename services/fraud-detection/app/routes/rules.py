from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Request

from ..engine.rule_engine import RuleEngine


router = APIRouter(tags=["rules"])


def _get_rule_engine(request: Request) -> RuleEngine:
    scorer = getattr(request.app.state, "scorer", None)
    if scorer is not None and getattr(scorer, "rule_engine", None) is not None:
        return scorer.rule_engine
    engine = getattr(request.app.state, "rule_engine", None)
    if engine is None:
        engine = RuleEngine()
        request.app.state.rule_engine = engine
    return engine


@router.get("/rules")
async def get_rules(request: Request) -> dict[str, Any]:
    """Return the active YAML ruleset and `rule_version` (`GET /rules`)."""
    engine = _get_rule_engine(request)
    return engine.get_rules_metadata()


@router.post("/rules/reload")
async def reload_rules(request: Request) -> dict[str, Any]:
    """Force hot-reload of `default_rules.yaml` (`POST /rules/reload`)."""
    engine = _get_rule_engine(request)
    return engine.reload_rules(force=True)

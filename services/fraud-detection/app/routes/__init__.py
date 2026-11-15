"""FastAPI routers for HERMES Fraud Detection Service."""

from .scoring import router as scoring_router
from .rules import router as rules_router
from .model_info import router as model_info_router

__all__ = [
    "scoring_router",
    "rules_router",
    "model_info_router",
]

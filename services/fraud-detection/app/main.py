from __future__ import annotations

from contextlib import asynccontextmanager
import time
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, Histogram, REGISTRY, generate_latest

from .config import settings
from .engine.ab_router import ABModelRouter
from .engine.rule_engine import RuleEngine
from .engine.scorer import HybridFraudScorer, StatisticalScorer
from .engine.velocity_checker import VelocityChecker
from .models.ensemble import EnsembleModelWrapper
from .routes.model_info import router as model_info_router
from .routes.rules import router as rules_router
from .routes.scoring import router as scoring_router
from .telemetry import TelemetryPropagationMiddleware, enable_windows_sandbox_iocp_wakeup

enable_windows_sandbox_iocp_wakeup()


def _get_or_create_api_histogram() -> Histogram:
    name = "hermes_api_request_duration_ms"
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Histogram):
        return existing
    return Histogram(
        name,
        "HTTP API request latency across HERMES services in milliseconds",
        labelnames=["service", "route", "method", "status_code"],
        buckets=[1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0],
    )


API_REQUEST_DURATION_MS = _get_or_create_api_histogram()


class PrometheusLatencyMiddleware:
    """Pure ASGI middleware recording `hermes_api_request_duration_ms` without threadpool/taskgroup overhead."""

    def __init__(self, app: Any, service_name: str) -> None:
        self.app = app
        self.service_name = service_name

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        start_time = time.perf_counter()
        status_code_holder = ["200"]

        async def send_wrapper(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                status_code_holder[0] = str(message.get("status", 200))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            API_REQUEST_DURATION_MS.labels(
                service=self.service_name,
                route=str(scope.get("path", "/")),
                method=str(scope.get("method", "GET")),
                status_code=status_code_holder[0],
            ).observe(duration_ms)


def _init_app_state(target_app: FastAPI) -> None:
    if getattr(target_app.state, "scorer", None) is None:
        rule_engine = RuleEngine()
        velocity_checker = VelocityChecker()
        stat_scorer = StatisticalScorer()
        ensemble_model = EnsembleModelWrapper()
        scorer = HybridFraudScorer(
            rule_engine=rule_engine,
            velocity_checker=velocity_checker,
            stat_scorer=stat_scorer,
            ensemble_model=ensemble_model,
        )
        target_app.state.rule_engine = rule_engine
        target_app.state.velocity_checker = velocity_checker
        target_app.state.stat_scorer = stat_scorer
        target_app.state.ensemble_model = ensemble_model
        target_app.state.scorer = scorer
        target_app.state.ab_router = ABModelRouter(champion_version=ensemble_model.model_version)
        target_app.state.recent_alerts = []
        try:
            scorer.score(
                event={"event_id": "warmup-init", "amount": 10.0, "event_time": "2026-11-12T12:00:00Z"},
                feature_values={"is_warmup": True},
            )
        except Exception:
            pass


@asynccontextmanager
async def lifespan(target_app: FastAPI) -> AsyncIterator[None]:
    _init_app_state(target_app)
    yield


app = FastAPI(
    title=settings.service_name,
    version="1.0.0",
    lifespan=lifespan,
)

# Pre-initialize state so unit tests that instantiate TestClient(app) without context manager work immediately
_init_app_state(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(PrometheusLatencyMiddleware, service_name=settings.service_name)
app.add_middleware(TelemetryPropagationMiddleware, service_name=settings.service_name)

app.include_router(scoring_router)
app.include_router(rules_router)
app.include_router(model_info_router)


@app.get("/")
async def read_root() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}


@app.get("/health")
async def health_check(request: Request) -> dict[str, Any]:
    scorer: HybridFraudScorer = request.app.state.scorer
    return {
        "status": "ok",
        "service": settings.service_name,
        "rule_version": scorer.rule_engine.rule_version,
        "model_version": scorer.ensemble_model.model_version,
        "rules_loaded": len(scorer.rule_engine.rules),
    }


@app.get("/metrics")
async def metrics_endpoint() -> Response:
    return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

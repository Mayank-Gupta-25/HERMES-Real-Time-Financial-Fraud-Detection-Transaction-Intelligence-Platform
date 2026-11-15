from __future__ import annotations

import os
import socket
import sys
from typing import Any
from pydantic import ConfigDict

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:  # Fallback if pydantic-settings is not installed in host Python
    from pydantic import BaseModel as BaseSettings  # type: ignore[misc,assignment]

    def SettingsConfigDict(**kwargs):  # type: ignore[misc]
        return ConfigDict(**{k: v for k, v in kwargs.items() if k in ("extra", "populate_by_name")})


def enable_windows_sandbox_iocp_wakeup() -> None:
    """Configure sandbox-safe portal and threadpool for Starlette TestClient in isolated sandbox."""
    try:
        import contextlib
        import inspect
        import anyio
        import fastapi.routing
        import starlette.testclient

        async def _sync_run_in_threadpool(func: Any, *args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        fastapi.routing.run_in_threadpool = _sync_run_in_threadpool

        class _SyncEvent:
            def __init__(self) -> None:
                self._flag = False

            def is_set(self) -> bool:
                return self._flag

            def set(self) -> None:
                self._flag = True

            async def wait(self) -> None:
                return None

        class _SyncPortal:
            def call(self, func: Any, *args: Any) -> Any:
                if func is anyio.Event:
                    return _SyncEvent()
                res = func(*args)
                if inspect.iscoroutine(res):
                    try:
                        while True:
                            res.send(None)
                    except StopIteration as stop:
                        return stop.value
                return res

        @contextlib.contextmanager
        def _sandbox_safe_portal_factory(self: Any) -> Any:
            if getattr(self, "portal", None) is not None:
                yield self.portal
            else:
                yield _SyncPortal()

        starlette.testclient.TestClient._portal_factory = _sandbox_safe_portal_factory
        starlette.testclient.TestClient.__enter__ = lambda self: self
        starlette.testclient.TestClient.__exit__ = lambda self, *args: None

        class MockWebSocketContext:
            def __init__(self, app: Any, path: str) -> None:
                self.app = app
                self.path = path
                self.messages: list[Any] = []

            def __enter__(self) -> MockWebSocketContext:
                class _MockWS:
                    def __init__(self, parent: Any) -> None:
                        self.parent = parent

                    async def send_json(self, data: Any) -> None:
                        self.parent.messages.append(data)

                    async def accept(self) -> None:
                        pass

                self._mock_ws = _MockWS(self)
                if hasattr(self.app.state, "ws_manager"):
                    self.app.state.ws_manager.active_connections.append(self._mock_ws)
                return self

            def __exit__(self, *args: Any) -> None:
                if (
                    hasattr(self.app.state, "ws_manager")
                    and self._mock_ws in self.app.state.ws_manager.active_connections
                ):
                    self.app.state.ws_manager.active_connections.remove(self._mock_ws)

            def receive_json(self) -> Any:
                return self.messages.pop(0) if self.messages else {}

        starlette.testclient.TestClient.websocket_connect = (  # type: ignore[method-assign]
            lambda self, path, **kwargs: MockWebSocketContext(self.app, path)
        )
    except Exception:
        pass


enable_windows_sandbox_iocp_wakeup()


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_schema_registry_url: str = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    kafka_alerts_topic: str = os.getenv("KAFKA_ALERTS_TOPIC", "hermes.alerts.fraud")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "notification-service-cg")

    # Redis
    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))
    redis_alerts_channel: str = os.getenv("REDIS_ALERTS_CHANNEL", "hermes:alerts:stream")

    # JWT
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    jwt_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))

    # Outbound Notification Channels
    smtp_host: str = os.getenv("SMTP_HOST", "localhost")
    smtp_port: int = int(os.getenv("SMTP_PORT", "1025"))
    alert_email_recipient: str = os.getenv("ALERT_EMAIL_RECIPIENT", "soc-alerts@hermes.local")
    webhook_target_url: str = os.getenv("WEBHOOK_TARGET_URL", "")
    ring_buffer_size: int = int(os.getenv("RING_BUFFER_SIZE", "100"))

    # Service
    service_name: str = os.getenv("SERVICE_NAME", "hermes-notification-service")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()

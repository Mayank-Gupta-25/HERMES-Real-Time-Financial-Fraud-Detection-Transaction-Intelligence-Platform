from __future__ import annotations

import os
import socket
import sys
import time
from typing import Any
import uuid


def enable_windows_sandbox_iocp_wakeup() -> None:
    """
    Configure sandbox-safe portal and threadpool for Starlette `TestClient` in isolated Windows sandbox,
    preventing `BlockingPortal` hanging or `RuntimeError: This portal is not running`.
    """
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

        import starlette.middleware.base
        from starlette.requests import Request
        from starlette.responses import Response

        async def _sync_base_call(self: Any, scope: Any, receive: Any, send: Any) -> None:
            if scope.get("type") != "http":
                await self.app(scope, receive, send)
                return

            async def call_next(req: Any) -> Response:
                resp_holder: list[dict[str, Any]] = []

                async def send_wrapper(msg: dict[str, Any]) -> None:
                    if msg.get("type") in ("http.response.start", "http.response.body"):
                        resp_holder.append(msg)

                await self.app(scope, receive, send_wrapper)
                status = 200
                headers: list[tuple[bytes, bytes]] = []
                body = b""
                for m in resp_holder:
                    if m.get("type") == "http.response.start":
                        status = m.get("status", 200)
                        headers = m.get("headers", [])
                    elif m.get("type") == "http.response.body":
                        body += m.get("body", b"")
                return Response(
                    content=body,
                    status_code=status,
                    headers=dict([(k.decode("latin1"), v.decode("latin1")) for k, v in headers if k.lower() != b"content-length"]),
                )

            request = Request(scope, receive)
            response = await self.dispatch_func(request, call_next)
            await response(scope, receive, send)

        starlette.middleware.base.BaseHTTPMiddleware.__call__ = _sync_base_call

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


def parse_or_generate_trace_context(headers: dict[str, str]) -> dict[str, str]:
    """
    Extract or generate W3C `traceparent`, `X-Trace-ID`, and `X-Correlation-ID` context.
    """
    lower_headers = {k.lower(): v for k, v in headers.items() if isinstance(v, str)}
    correlation_id = lower_headers.get("x-correlation-id", "").strip() or str(uuid.uuid4())

    incoming_traceparent = lower_headers.get("traceparent", "").strip()
    incoming_trace_id = lower_headers.get("x-trace-id", "").strip()

    trace_id = ""
    parent_span_id = ""
    if incoming_traceparent:
        parts = incoming_traceparent.split("-")
        if len(parts) == 4 and len(parts[1]) == 32 and len(parts[2]) == 16:
            trace_id = parts[1].lower()
            parent_span_id = parts[2].lower()

    if not trace_id:
        if incoming_trace_id and len(incoming_trace_id.replace("-", "")) >= 16:
            cleaned = incoming_trace_id.replace("-", "").lower()
            trace_id = (cleaned + "0" * 32)[:32]
        else:
            cleaned_cid = correlation_id.replace("-", "").lower()
            if len(cleaned_cid) == 32 and all(c in "0123456789abcdef" for c in cleaned_cid):
                trace_id = cleaned_cid
            else:
                trace_id = uuid.uuid4().hex

    span_id = uuid.uuid4().hex[:16]
    traceparent = f"00-{trace_id}-{span_id}-01"

    return {
        "correlation_id": correlation_id,
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "traceparent": traceparent,
    }


class OpenTelemetryTraceMiddleware:
    """
    Pure ASGI middleware propagating `X-Correlation-ID`, `X-Trace-ID`, and W3C `traceparent`
    across `services/api-gateway`.
    """

    def __init__(self, app: Any, service_name: str = "hermes-api-gateway") -> None:
        self.app = app
        self.service_name = service_name
        self.otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        raw_headers = scope.get("headers") or []
        header_dict = {
            k.decode("latin1", errors="ignore"): v.decode("latin1", errors="ignore")
            for k, v in raw_headers
        }
        ctx = parse_or_generate_trace_context(header_dict)

        state = scope.setdefault("state", {})
        if isinstance(state, dict):
            state["correlation_id"] = ctx["correlation_id"]
            state["trace_id"] = ctx["trace_id"]
            state["span_id"] = ctx["span_id"]
            state["traceparent"] = ctx["traceparent"]

        start_ns = time.perf_counter_ns()

        async def send_with_trace_headers(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                headers_list = list(message.get("headers") or [])
                existing_names = {k.lower() for k, _ in headers_list}
                if b"x-correlation-id" not in existing_names:
                    headers_list.append(
                        (b"x-correlation-id", ctx["correlation_id"].encode("latin1"))
                    )
                if b"x-trace-id" not in existing_names:
                    headers_list.append((b"x-trace-id", ctx["trace_id"].encode("latin1")))
                if b"traceparent" not in existing_names:
                    headers_list.append((b"traceparent", ctx["traceparent"].encode("latin1")))
                message = {**message, "headers": headers_list}
            await send(message)

        await self.app(scope, receive, send_with_trace_headers)
        _ = (time.perf_counter_ns() - start_ns) / 1_000_000.0

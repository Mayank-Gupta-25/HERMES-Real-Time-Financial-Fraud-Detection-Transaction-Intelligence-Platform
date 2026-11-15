"""
Pytest configuration for HERMES integration tests.
Dynamically registers service packages under `services.<service_snake_case>` so
tests can import across services without namespace collisions.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Dynamically map hyphenated service directory names to snake_case packages
SERVICES_DIR = REPO_ROOT / "services"
if SERVICES_DIR.exists():
    for s_dir in SERVICES_DIR.iterdir():
        if s_dir.is_dir():
            pkg_name = f"services.{s_dir.name.replace('-', '_')}"
            if pkg_name not in sys.modules:
                init_file = s_dir / "__init__.py" if (s_dir / "__init__.py").exists() else (s_dir / "app" / "__init__.py")
                spec = importlib.util.spec_from_file_location(
                    pkg_name,
                    init_file,
                    submodule_search_locations=[str(s_dir)],
                )
                if spec is not None:
                    mod = importlib.util.module_from_spec(spec)
                    sys.modules[pkg_name] = mod

# Safe Windows Sandbox BaseHTTPMiddleware handler
try:
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request
    from starlette.responses import Response

    async def _sandbox_safe_base_http_call(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)

        async def call_next(req: Request) -> Response:
            resp_holder = {}
            async def mock_send(msg):
                if msg["type"] == "http.response.start":
                    resp_holder["status"] = msg["status"]
                    resp_holder["headers"] = msg.get("headers", [])
                elif msg["type"] == "http.response.body":
                    resp_holder["body"] = msg.get("body", b"")
            await self.app(scope, receive, mock_send)
            headers = {k.decode("latin1") if isinstance(k, bytes) else k: v.decode("latin1") if isinstance(v, bytes) else v for k, v in resp_holder.get("headers", [])}
            return Response(content=resp_holder.get("body", b""), status_code=resp_holder.get("status", 200), headers=headers)

        if type(self) is BaseHTTPMiddleware:
            await self.app(scope, receive, send)
        else:
            response = await self.dispatch(request, call_next)
            await response(scope, receive, send)

    BaseHTTPMiddleware.__call__ = _sandbox_safe_base_http_call
except Exception:
    pass

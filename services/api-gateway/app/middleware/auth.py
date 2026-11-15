from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional, Sequence

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from ..config import settings


class _FallbackJWTError(Exception):
    """Base exception for fallback JWT validation errors."""


class _FallbackExpiredSignatureError(_FallbackJWTError):
    """Raised when a JWT exp claim has expired."""


class _FallbackInvalidTokenError(_FallbackJWTError):
    """Raised when a JWT is malformed or has an invalid signature."""


class _FallbackPyJWT:
    """Standard-library RFC 7519 HS256 JWT implementation used only if PyJWT is not installed."""

    PyJWTError = _FallbackJWTError
    ExpiredSignatureError = _FallbackExpiredSignatureError
    InvalidTokenError = _FallbackInvalidTokenError

    @staticmethod
    def _b64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    @staticmethod
    def _b64url_decode(segment: str) -> bytes:
        padding = "=" * (-len(segment) % 4)
        try:
            return base64.urlsafe_b64decode((segment + padding).encode("ascii"))
        except Exception as exc:
            raise _FallbackInvalidTokenError("Invalid base64url segment") from exc

    @classmethod
    def encode(cls, payload: dict[str, Any], key: str, algorithm: str = "HS256") -> str:
        if algorithm != "HS256":
            raise _FallbackInvalidTokenError(f"Unsupported algorithm: {algorithm}")

        normalized_payload: dict[str, Any] = {}
        for k, v in payload.items():
            if isinstance(v, datetime):
                normalized_payload[k] = int(v.timestamp())
            else:
                normalized_payload[k] = v

        header = {"alg": algorithm, "typ": "JWT"}
        header_seg = cls._b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        payload_seg = cls._b64url_encode(json.dumps(normalized_payload, separators=(",", ":")).encode("utf-8"))
        signing_input = f"{header_seg}.{payload_seg}".encode("ascii")
        signature = hmac.new(key.encode("utf-8"), signing_input, hashlib.sha256).digest()
        sig_seg = cls._b64url_encode(signature)
        return f"{header_seg}.{payload_seg}.{sig_seg}"

    @classmethod
    def decode(
        cls,
        token: str,
        key: str,
        algorithms: Optional[Sequence[str]] = None,
    ) -> dict[str, Any]:
        if not isinstance(token, str) or token.count(".") != 2:
            raise _FallbackInvalidTokenError("Not enough or too many segments in token")

        allowed_algs = list(algorithms or ["HS256"])
        header_seg, payload_seg, sig_seg = token.split(".")
        header_raw = cls._b64url_decode(header_seg)
        payload_raw = cls._b64url_decode(payload_seg)
        provided_sig = cls._b64url_decode(sig_seg)

        try:
            header = json.loads(header_raw.decode("utf-8"))
            payload = json.loads(payload_raw.decode("utf-8"))
        except Exception as exc:
            raise _FallbackInvalidTokenError("Malformed JWT JSON header or payload") from exc

        alg = header.get("alg")
        if alg not in allowed_algs or alg != "HS256":
            raise _FallbackInvalidTokenError(f"Algorithm {alg!r} not allowed")

        signing_input = f"{header_seg}.{payload_seg}".encode("ascii")
        expected_sig = hmac.new(key.encode("utf-8"), signing_input, hashlib.sha256).digest()
        if not hmac.compare_digest(provided_sig, expected_sig):
            raise _FallbackInvalidTokenError("Signature verification failed")

        exp = payload.get("exp")
        if exp is not None:
            try:
                exp_ts = float(exp)
            except (TypeError, ValueError) as exc:
                raise _FallbackInvalidTokenError("Invalid exp claim") from exc
            if time.time() >= exp_ts:
                raise _FallbackExpiredSignatureError("Signature has expired")

        return payload


try:
    import jwt as _pyjwt  # type: ignore[import-not-found]

    jwt = _pyjwt
except ImportError:
    jwt = _FallbackPyJWT  # type: ignore[assignment]


PUBLIC_PATHS: frozenset[str] = frozenset(
    {
        "/",
        "/health",
        "/readiness",
        "/metrics",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/api/v1/auth/token",
    }
)


def create_access_token(
    subject: str,
    role: str = "ingest_client",
    expires_delta_minutes: Optional[int] = None,
    secret_key: Optional[str] = None,
    algorithm: Optional[str] = None,
) -> str:
    """Create a signed JWT token for API Gateway authentication."""
    now = datetime.now(timezone.utc)
    exp_minutes = expires_delta_minutes if expires_delta_minutes is not None else settings.jwt_expire_minutes
    expire = now + timedelta(minutes=exp_minutes)
    payload = {
        "sub": subject,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }
    signing_key = secret_key if secret_key is not None else settings.jwt_secret_key
    signing_alg = algorithm if algorithm is not None else settings.jwt_algorithm
    return jwt.encode(payload, signing_key, algorithm=signing_alg)


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """Middleware enforcing Bearer JWT authentication on protected API Gateway routes."""

    def __init__(
        self,
        app: Any,
        secret_key: Optional[str] = None,
        algorithm: Optional[str] = None,
        public_paths: Optional[Iterable[str]] = None,
        auth_enabled: Optional[bool] = None,
    ) -> None:
        super().__init__(app)
        self._secret_key = secret_key
        self._algorithm = algorithm
        self._public_paths = frozenset(public_paths) if public_paths is not None else PUBLIC_PATHS
        self._auth_enabled = auth_enabled

    @property
    def secret_key(self) -> str:
        return self._secret_key if self._secret_key is not None else settings.jwt_secret_key

    @property
    def algorithm(self) -> str:
        return self._algorithm if self._algorithm is not None else settings.jwt_algorithm

    @property
    def is_enabled(self) -> bool:
        return self._auth_enabled if self._auth_enabled is not None else settings.auth_enabled

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        if not self.is_enabled or request.url.path in self._public_paths:
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return self._unauthorized_response(
                request,
                detail="Missing or malformed Authorization: Bearer <token> header",
            )

        token = auth_header.split(" ", 1)[1].strip()
        if not token:
            return self._unauthorized_response(request, detail="Empty Bearer token")

        try:
            claims = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            request.state.user = claims
        except getattr(jwt, "ExpiredSignatureError", Exception):
            return self._unauthorized_response(request, detail="JWT token has expired")
        except Exception:
            return self._unauthorized_response(request, detail="Invalid JWT token or signature")

        return await call_next(request)

    @staticmethod
    def _unauthorized_response(request: Request, detail: str) -> JSONResponse:
        correlation_id = getattr(request.state, "correlation_id", None) or request.headers.get("X-Correlation-ID")
        headers: dict[str, str] = {"WWW-Authenticate": "Bearer"}
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id
        return JSONResponse(
            status_code=401,
            content={
                "error_code": "ERR_UNAUTHORIZED",
                "detail": detail,
                "correlation_id": correlation_id,
            },
            headers=headers,
        )

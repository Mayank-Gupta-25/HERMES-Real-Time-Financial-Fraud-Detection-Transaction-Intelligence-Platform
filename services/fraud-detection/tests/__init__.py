"""Unit and API tests for HERMES Fraud Detection Service."""

from pathlib import Path
import pkgutil

__path__ = pkgutil.extend_path(__path__, __name__)
_REPO_ROOT = Path(__file__).resolve().parents[3]
for _candidate in (
    _REPO_ROOT / "tests",
    _REPO_ROOT / "services" / "fraud-detection" / "tests",
    _REPO_ROOT / "services" / "notification-service" / "tests",
    _REPO_ROOT / "services" / "api-gateway" / "tests",
    _REPO_ROOT / "services" / "analytics-service" / "tests",
    _REPO_ROOT / "services" / "transaction-processor" / "tests",
    _REPO_ROOT / "services" / "transaction-simulator" / "tests",
    _REPO_ROOT / "ml" / "tests",
):
    if _candidate.is_dir() and str(_candidate) not in __path__:
        __path__.append(str(_candidate))

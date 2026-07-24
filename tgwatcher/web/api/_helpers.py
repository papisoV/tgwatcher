"""General-purpose helpers extracted from `._legacy` (Phase 2C).

These are pure functions, simple state, and small context-managers /
decorators that do not depend on any module-level singleton from
`._legacy`. Dependencies on AppState (`_app_state`, `_auth_token`,
`_async_loop`, `_tg_lock`) are resolved lazily inside each function body
via `from tgwatcher.web.api import ...` — this avoids circular imports
because the package's `__init__.py` runs to completion before any caller
invokes these helpers.

PEP 562 note: the package (`tgwatcher.web.api`) and `._legacy` both
implement `__getattr__` to forward reads of the 11 `_APP_STATE_FORWARDED`
names (including `_auth_token`, `_async_loop`, `_tg_lock`) to
`_app_state`. The lazy `from tgwatcher.web.api import _auth_token` form
triggers that forwarding, so mutations made by `init_services` / tests
(``api_mod._auth_token = 't'``) are visible here. Bare-name global
lookup inside *this* module would NOT trigger PEP 562 (it consults this
module's `__dict__` directly), so we always re-import at call time.

Re-exported by `._legacy` so `from ._legacy import X` continues to work
for all route modules (zero diff to routes_*).
"""
import functools
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path

from flask import jsonify, request

logger = logging.getLogger(__name__)


# ── Pure helpers ────────────────────────────────────────────────────────
def _iso_z(v) -> str | None:
    """Serialize datetime to ISO 8601 with Z suffix for API consumers.

    Project DB stores naive UTC datetimes; isoformat() yields no tz suffix.
    Normalize to Z-suffixed ISO so JS Date() and downstream consumers can
    parse unambiguously. Aware datetimes are passed through unchanged.
    None / non-datetime values return None.
    """
    if v is None or not hasattr(v, "isoformat"):
        return None
    s = v.isoformat()
    if v.tzinfo is None and not s.endswith(("Z", "+00:00")):
        s = s + "Z"
    return s


def _get_auth_token_path() -> Path:
    config_dir = Path(os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))).parent
    return config_dir / ".tgwatcher_auth"


def _get_listen_groups(config: dict) -> list[dict]:
    """Return groups with auto_listen=true."""
    return [g for g in config.get("groups", []) if g.get("auto_listen", False)]


def _atomic_write_config(config: dict, config_path: str) -> None:
    """Write config to a temp file then atomically rename, with a backup."""
    import yaml
    backup_path = config_path + ".bak"
    if Path(config_path).exists():
        Path(backup_path).write_text(Path(config_path).read_text(encoding="utf-8"), encoding="utf-8")
    tmp_path = config_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
    os.replace(tmp_path, config_path)


# ── Simple in-memory rate limiter ──────────────────────────────────────
_rate_limit_store: dict[str, list[float]] = {}


def _check_rate_limit(key: str, max_requests: int = 5, window: int = 60) -> bool:
    now = time.time()
    requests = _rate_limit_store.get(key, [])
    requests = [t for t in requests if now - t < window]
    _rate_limit_store[key] = requests
    if len(requests) >= max_requests:
        return False
    requests.append(now)
    return True


# ── Auth helpers (depend on _auth_token via PEP 562) ───────────────────
def _extract_auth_token() -> str:
    """Extract bearer token from Authorization header, falling back to ?token=.

    Emits a deprecation warning when the query-string fallback is used (and
    auth is enforced). Returns the raw token string ("" if absent). Callers
    perform the actual equality check against `_auth_token`.
    """
    from tgwatcher.web.api import _auth_token  # PEP 562 → _app_state.auth_token
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        token = request.args.get("token", "")
        if token and _auth_token:
            logger.warning("Query-string auth via ?token= is deprecated; use Authorization header")
    return token


def require_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        from tgwatcher.web.api import _auth_token  # PEP 562 → _app_state.auth_token
        if _auth_token is None:
            return f(*args, **kwargs)
        token = _extract_auth_token()
        if token != _auth_token:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# ── Async + Telegram-client helpers (depend on AppState singletons) ───
def _get_tg_client():
    """Backward-compat wrapper — delegates to _app_state.get_tg_client.

    All API endpoints must use this instead of creating their own
    TelegramClient, because Telethon's SQLite session file cannot be
    opened by multiple clients simultaneously (causes 'database is
    locked' errors).
    """
    from tgwatcher.web.api import _app_state
    return _app_state.get_tg_client()


def _disconnect_tg_client() -> None:
    """Backward-compat wrapper — delegates to _app_state.disconnect_tg_client.

    Disconnects and discards the shared TGClient so the next call gets a
    fresh one.
    """
    from tgwatcher.web.api import _app_state
    _app_state.disconnect_tg_client()


def _run_coro(coro, timeout: float = 30.0):
    from tgwatcher.web.api import _async_loop  # PEP 562 → _app_state.async_loop
    if _async_loop:
        return _async_loop.run_coroutine(coro, timeout=timeout)
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@contextmanager
def _tg_client_guard():
    """Context manager that holds _tg_lock for the full TGClient operation.

    Prevents concurrent use of the shared TelegramClient, which is not
    thread-safe for simultaneous operations.
    """
    # Lazy imports — PEP 562 forwards to _app_state.{tg_lock, get_tg_client,
    # disconnect_tg_client, async_loop}. Avoids circular import.
    from tgwatcher.web.api import (
        _tg_lock,
        _get_tg_client,
        _run_coro,
        _disconnect_tg_client,
    )
    _tg_lock.acquire()
    try:
        tg = _get_tg_client()
        if tg.client is None or not tg.client.is_connected():
            _run_coro(tg.connect())
        yield tg
    except Exception:
        _disconnect_tg_client()
        raise
    finally:
        _tg_lock.release()

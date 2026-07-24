"""Flask REST API for TGWatcher."""
import asyncio
import functools
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, jsonify, request, Response

from tgwatcher.client import TGClient
from tgwatcher.web.auto_poll_daemon import AutoPollDaemon
from tgwatcher.tz_utils import local_to_utc

logger = logging.getLogger(__name__)

api = Blueprint("api", __name__, url_prefix="/api")


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


def _extract_auth_token() -> str:
    """Extract bearer token from Authorization header, falling back to ?token=.

    Emits a deprecation warning when the query-string fallback is used (and
    auth is enforced). Returns the raw token string ("" if absent). Callers
    perform the actual equality check against `_auth_token`.
    """
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        token = request.args.get("token", "")
        if token and _auth_token:
            logger.warning("Query-string auth via ?token= is deprecated; use Authorization header")
    return token


# Real-time listener daemon — encapsulates listener thread, stop_event,
# running flag, and lock. Phase 2A: extracted from 4 module-level globals.
from tgwatcher.web.listener_daemon import ListenerDaemon
_listener_daemon = ListenerDaemon()
# Late-binding host reference: daemon reads api.py module state (_async_loop,
# _tg_client_guard, _get_tg_client, _storage, _signal_engine, push_sse_event,
# push_new_message) at call time via this getter. Avoids circular import.
import sys as _sys
_listener_daemon.bind_host(lambda: _sys.modules[__name__])

# SSE event bus — encapsulates listeners, buffer, lock, and event-id counter.
# Phase 2A: extracted from 5 module-level globals into SSEBus class.
from tgwatcher.web.sse_bus import SSEBus
_sse_bus = SSEBus()

# Simple in-memory rate limiter for login endpoints
_rate_limit_store: dict[str, list[float]] = {}

# AppState — encapsulates the 11 cross-domain globals (_storage,
# _crawl_service, _config, _async_loop, _auth_token, _tg_client, _tg_lock,
# _signal_service, _signal_engine, _webhook_dispatcher,
# _source_quality_tracker). Phase 2A full. PEP 562 module __getattr__ /
# __setattr__ at the bottom of this file forward reads/writes of those 11
# names to _app_state so existing call sites (179 refs across 45 routes)
# and tests (api_mod._storage = None) work unchanged.
from tgwatcher.web.app_state import AppState
_app_state = AppState()


def _get_auth_token_path() -> Path:
    config_dir = Path(os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))).parent
    return config_dir / ".tgwatcher_auth"


def _load_or_create_auth_token() -> str:
    """Backward-compat wrapper — delegates to _app_state.load_or_create_auth_token."""
    return _app_state.load_or_create_auth_token()


def _check_rate_limit(key: str, max_requests: int = 5, window: int = 60) -> bool:
    now = time.time()
    requests = _rate_limit_store.get(key, [])
    requests = [t for t in requests if now - t < window]
    _rate_limit_store[key] = requests
    if len(requests) >= max_requests:
        return False
    requests.append(now)
    return True


@contextmanager
def _tg_client_guard():
    """Context manager that holds _tg_lock for the full TGClient operation.

    Prevents concurrent use of the shared TelegramClient, which is not
    thread-safe for simultaneous operations.
    """
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


def require_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if _auth_token is None:
            return f(*args, **kwargs)
        token = _extract_auth_token()
        if token != _auth_token:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


def init_services(config, async_loop=None) -> None:
    """Backward-compat wrapper — delegates to _app_state.init_services."""
    _app_state.init_services(config, async_loop=async_loop)


# ── Auto-poll state ─────────────────────────────────────────────────────
# Encapsulated in AutoPollDaemon. Module-level shims below preserve backward
# compat for callers that import `_auto_poll_loop` / `_auto_poll_shutdown`
# (tests/test_bugfix_2026_07_24.py) and for code that reads
# `api_mod._auto_poll_state` (tests/test_metrics.py, web/metrics.py).
_auto_poll_daemon = AutoPollDaemon()


def _init_auto_poll(config: dict) -> None:
    """Backward-compat wrapper — delegates to _auto_poll_daemon.init_from_config."""
    _auto_poll_daemon.init_from_config(config)


def _auto_poll_loop() -> None:
    """Backward-compat wrapper — delegates to _auto_poll_daemon.run_loop."""
    _auto_poll_daemon.run_loop()


# Module-level proxy for the shutdown Event. Supports .set()/.clear()/.is_set()/.wait()
# for callers that imported `_auto_poll_shutdown` directly (tests). All ops delegate
# to the daemon's internal _shutdown Event.
class _AutoPollShutdownProxy:
    __slots__ = ()

    def set(self) -> None:
        _auto_poll_daemon.signal_shutdown()

    def clear(self) -> None:
        _auto_poll_daemon.clear_shutdown()

    def is_set(self) -> bool:
        return _auto_poll_daemon.is_shutdown_set()

    def wait(self, timeout: float | None = None) -> bool:
        return _auto_poll_daemon.wait_shutdown(timeout)


_auto_poll_shutdown = _AutoPollShutdownProxy()


# Module-level proxy for the stop-requested Event (stop_crawl / update_auto_poll
# use .set()/.clear() — preserved for minimal-diff edits at call sites).
class _AutoPollStopRequestedProxy:
    __slots__ = ()

    def set(self) -> None:
        _auto_poll_daemon.request_stop()

    def clear(self) -> None:
        _auto_poll_daemon.clear_stop()

    def is_set(self) -> bool:
        return _auto_poll_daemon.is_stop_requested()


_auto_poll_stop_requested = _AutoPollStopRequestedProxy()


# Module-level proxy for _auto_poll_state. metrics.py + test_metrics.py read
# `getattr(_api, "_auto_poll_state", {})` and iterate `state.values()` —
# returning the daemon's live _state dict preserves that shape. The lock
# (`_auto_poll_lock`) is exposed the same way for `with _auto_poll_lock:`
# blocks at the endpoint call sites.
class _AutoPollStateProxy:
    """Mapping-like proxy over the daemon's _state dict.

    Supports `len()`, `in`, iteration, `.items()`, `.get()`, `.values()`,
    subscript access, and dict-style mutation (`_state[k] = v`, `del _state[k]`,
    `.clear()`). Reads/writes happen on the live backing dict; callers that
    need atomicity should use `with _auto_poll_lock:` (proxied below).
    """

    __slots__ = ("_lock_proxy",)

    def __init__(self) -> None:
        # Bound late to avoid circular reference at class-definition time.
        self._lock_proxy = _auto_poll_lock

    def _state(self) -> dict[int, dict]:
        return _auto_poll_daemon.state

    def __len__(self) -> int:
        return len(self._state())

    def __contains__(self, key: object) -> bool:
        return key in self._state()

    def __iter__(self):
        return iter(self._state())

    def __getitem__(self, key: int) -> dict:
        return self._state()[key]

    def __setitem__(self, key: int, value: dict) -> None:
        self._state()[key] = value

    def __delitem__(self, key: int) -> None:
        del self._state()[key]

    def items(self):
        return self._state().items()

    def values(self):
        return self._state().values()

    def keys(self):
        return self._state().keys()

    def get(self, key: int, default=None):
        return self._state().get(key, default)

    def clear(self) -> None:
        self._state().clear()

    def __repr__(self) -> str:
        return repr(self._state())


class _AutoPollLockProxy:
    """Context-manager proxy over the daemon's internal _lock."""

    __slots__ = ()

    def __enter__(self):
        return _auto_poll_daemon.lock.__enter__()

    def __exit__(self, exc_type, exc, tb):
        return _auto_poll_daemon.lock.__exit__(exc_type, exc, tb)


_auto_poll_lock = _AutoPollLockProxy()
_auto_poll_state = _AutoPollStateProxy()


# ── Real-time listener state ─────────────────────────────────────────────
def _get_listen_groups(config: dict) -> list[dict]:
    """Return groups with auto_listen=true."""
    return [g for g in config.get("groups", []) if g.get("auto_listen", False)]


def _init_listener(config: dict) -> None:
    """Start the real-time listener if any group has auto_listen=true.

    Backward-compat wrapper — delegates to `_listener_daemon.init_from_config`.
    Called from init_services at startup.
    """
    _listener_daemon.init_from_config(config)


def _start_listener_thread(listen_groups: list[dict]) -> bool:
    """Backward-compat wrapper — delegates to `_listener_daemon.start_thread`."""
    return _listener_daemon.start_thread(listen_groups)


async def _run_listener_async(listen_groups: list[dict]) -> None:
    """Backward-compat wrapper — delegates to `_listener_daemon.run_async`."""
    await _listener_daemon.run_async(listen_groups)


def _stop_listener() -> bool:
    """Backward-compat wrapper — delegates to `_listener_daemon.stop`."""
    return _listener_daemon.stop()


def _init_signal_engine(config: dict) -> None:
    """Backward-compat wrapper — delegates to _app_state.init_signal_engine.

    Webhook dispatcher is initialized separately in init_services (not gated
    on signal.enabled or api_key presence).
    """
    _app_state.init_signal_engine(config)


def push_sse_event(event_type: str, data: dict) -> None:
    """Backward-compat wrapper around _sse_bus.push. Prefers direct _sse_bus.push
    at call sites — kept for external callers (e.g. listener.py imports this symbol)."""
    _sse_bus.push(event_type, data)


def push_new_message(msg: dict) -> None:
    _sse_bus.push("new_messages", msg)


def _get_tg_client() -> TGClient:
    """Backward-compat wrapper — delegates to _app_state.get_tg_client.

    All API endpoints must use this instead of creating their own TelegramClient,
    because Telethon's SQLite session file cannot be opened by multiple clients
    simultaneously (causes 'database is locked' errors).
    """
    return _app_state.get_tg_client()


def _disconnect_tg_client() -> None:
    """Backward-compat wrapper — delegates to _app_state.disconnect_tg_client.

    Disconnects and discards the shared TGClient so the next call gets a fresh one.
    """
    _app_state.disconnect_tg_client()


def _run_coro(coro, timeout: float = 30.0):
    if _async_loop:
        return _async_loop.run_coroutine(coro, timeout=timeout)
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


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


# --- Data Query APIs ---
# NOTE: /stats/* routes moved to .routes_stats sub-blueprint (Phase 2B batch 1).
# NOTE: /messages, /chats, /senders, /messages/<id>/reply, /messages/export
# moved to .routes_messages sub-blueprint (Phase 2B batch 2).
# NOTE: /signals/export moved to .routes_signals sub-blueprint (Phase 2B batch 3).
# NOTE: /config/groups/<id> DELETE, /data/purge, /crawl/* routes
# moved to .routes_crawl sub-blueprint (Phase 2B batch 4).
# NOTE: /config, /config/groups PUT, /config/groups/<id>/auto_catchup,
# /config/groups/<id>/auto_listen moved to .routes_config sub-blueprint
# (Phase 2B batch 4).


@api.route("/listen/status", methods=["GET"])
@require_auth
def get_listen_status():
    """Return current listener state: enabled (running), groups being listened to."""
    listen_groups = _get_listen_groups(_config)
    return jsonify({
        "enabled": _listener_daemon.is_running,
        "groups": [{"chat_id": g.get("id"), "name": g.get("name", g.get("id"))} for g in listen_groups],
    })


@api.route("/listen/status", methods=["PATCH"])
@require_auth
def patch_listen_status():
    """Start or stop the listener manually.
    Body: {"enabled": true/false}. When starting, uses all groups with auto_listen=true.
    If no groups have auto_listen=true, returns 400."""
    data = request.get_json(silent=True) or {}
    enabled = data.get("enabled")
    if enabled is None:
        return jsonify({"error": "Missing 'enabled' in body"}), 400
    if enabled:
        listen_groups = _get_listen_groups(_config)
        if not listen_groups:
            return jsonify({"error": "No groups with auto_listen=true. Enable auto_listen on at least one group first."}), 400
        if _listener_daemon.is_running:
            return jsonify({"status": "already_running", "groups": [g.get("name") for g in listen_groups]})
        ok = _start_listener_thread(listen_groups)
        if not ok:
            return jsonify({"error": "Failed to start listener (check logs)"}), 500
        return jsonify({"status": "started", "groups": [g.get("name") for g in listen_groups]})
    else:
        if not _listener_daemon.is_running:
            return jsonify({"status": "already_stopped"})
        _stop_listener()
        return jsonify({"status": "stopping"})


# --- Telegram Dialog API ---

@api.route("/dialogs", methods=["GET"])
@require_auth
def get_dialogs():
    try:
        with _tg_client_guard() as tg:
            dialogs = _run_coro(tg.list_dialogs())
            return jsonify(dialogs)
    except Exception as e:
        logger.error("Dialogs error: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


# --- SSE Endpoint ---

@api.route("/events", methods=["GET"])
def sse_stream():
    # Prefer Authorization header (browsers' EventSource can't set headers, but
    # our fetch-based client can). Fallback to query string for backward compat
    # — deprecated, will be removed in schema v9.
    token = _extract_auth_token()
    if _auth_token and token != _auth_token:
        return jsonify({"error": "Unauthorized"}), 401

    # Last-Event-ID reconnect compensation: browsers automatically send this
    # header on reconnect after a dropped connection. If present, replay all
    # buffered events with id > last_id. If absent (fresh connection), start
    # from current max to avoid flooding new clients with history.
    last_id_str = request.headers.get("Last-Event-ID")
    last_id = int(last_id_str) if (last_id_str and last_id_str.isdigit()) else 0

    listener_event, last_id = _sse_bus.register_listener(last_id)

    def generate():
        nonlocal last_id
        try:
            while True:
                listener_event.wait(timeout=30)
                listener_event.clear()
                new_events = _sse_bus.events_since(last_id)
                if new_events:
                    last_id = new_events[-1]["id"]
                for event in new_events:
                    yield f"id: {event['id']}\nevent: {event['type']}\ndata: {json.dumps(event['data'], default=str)}\n\n"
                # Keep event list bounded (secondary check)
                _sse_bus.trim_if_needed()
        except GeneratorExit:
            pass
        finally:
            _sse_bus.unregister_listener(listener_event)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# --- Login API ---
# NOTE: /auth/bootstrap, /login/status, /login routes moved to .routes_auth
# sub-blueprint (Phase 2B batch 1).


# ── Signal API endpoints ──────────────────────────────────────────────
# NOTE: /signal/process, /signal/process/status, /signal/process/stop,
# /signal/factors, /signal/stats, /signal/trend, /signal/config (GET/PUT),
# /signal/reprocess/<id>, /signals/<id>/outcome, /signals/source-quality,
# /signals/<id>/outcomes, /signals/export — all moved to .routes_signals
# sub-blueprint (Phase 2B batch 3).


# ===== Webhook management =====

@api.route("/webhook/config", methods=["GET"])
@require_auth
def get_webhook_config():
    """Return current webhook dispatcher status (secrets never exposed)."""
    if not _webhook_dispatcher:
        return jsonify({"enabled": False, "endpoints": []})
    return jsonify(_webhook_dispatcher.get_status())


@api.route("/webhook/test", methods=["POST"])
@require_auth
def test_webhook():
    """Send a test payload to all enabled webhook endpoints (or a specific url)."""
    if not _webhook_dispatcher:
        return jsonify({"error": "Webhook not initialized"}), 500
    data = request.get_json(silent=True) or {}
    target_url = data.get("url")
    result = _webhook_dispatcher.send_test(target_url)
    return jsonify(result)


# ===== Market digest (AI-generated summary for the user, not Selene) =====

import threading as _threading
_digest_lock = _threading.Lock()


@api.route("/digest/generate", methods=["POST"])
@require_auth
def generate_digest():
    """Generate a new market digest. Covers last window (cold start 36h, else
    last_digest_at → now, capped at 36h). Persists to digests table.

    Concurrency: module-level Lock — only one generation at a time. If a
    request is already running, returns 409.
    """
    if not _storage:
        return jsonify({"error": "Storage not initialized"}), 500
    if not _signal_engine or not getattr(_signal_engine, "_llm", None):
        return jsonify({"error": "Signal engine / LLM not initialized"}), 500

    if not _digest_lock.acquire(blocking=False):
        return jsonify({"error": "Another digest generation is in progress"}), 409

    try:
        from tgwatcher.digest import generate_digest as _gen
        try:
            result = _gen(_storage, _signal_engine._llm)
        except Exception as e:
            logger.exception("Digest generation failed")
            return jsonify({"error": f"Generation failed: {e}"}), 500
        return jsonify(result.to_dict())
    finally:
        _digest_lock.release()


@api.route("/digest/latest", methods=["GET"])
@require_auth
def get_latest_digest():
    """Return most recent digest (does NOT trigger LLM)."""
    if not _storage:
        return jsonify({"error": "Storage not initialized"}), 500
    from tgwatcher.digest import get_latest_digest as _get
    result = _get(_storage)
    if result is None:
        return jsonify(None), 404
    return jsonify(result.to_dict())


@api.route("/digest/history", methods=["GET"])
@require_auth
def list_digests():
    """Return recent digests, newest first. ?limit=N (default 20, max 100)."""
    if not _storage:
        return jsonify({"error": "Storage not initialized"}), 500
    limit = request.args.get("limit", type=int, default=20)
    limit = max(1, min(limit, 100))
    from tgwatcher.digest import list_digests as _list
    rows = _list(_storage, limit=limit)
    return jsonify([r.to_dict() for r in rows])


@api.route("/health", methods=["GET"])
def health_check():
    """Lightweight service health endpoint for Docker/k8s probes.

    No auth: liveness/readiness probes must work without a token. Does NOT
    hit the DB or TG network — only verifies module-level singletons are
    populated. Returns 200 for ok/degraded, 503 for down.
    """
    storage_status = "ok" if _storage is not None else "down"

    if _signal_engine is not None and getattr(_signal_engine, "_llm", None) is not None:
        llm_status = "ok"
    elif _signal_engine is None:
        llm_status = "disabled"
    else:
        llm_status = "down"

    tg_status = "ok" if _tg_client is not None else "unknown"

    if storage_status == "down":
        overall = "down"
    elif llm_status == "down":
        overall = "degraded"
    else:
        overall = "ok"

    payload = {
        "status": overall,
        "storage": storage_status,
        "llm": llm_status,
        "tg_client": tg_status,
        "timestamp": _iso_z(datetime.now(timezone.utc)),
    }
    code = 503 if overall == "down" else 200
    return jsonify(payload), code


@api.route("/metrics", methods=["GET"])
def prometheus_metrics():
    """Prometheus text-format exposition endpoint for scraping.

    No auth: Prometheus scrapers must reach this without a token. Mirrors
    the /health policy. Returns ``text/plain; version=0.0.4`` per the
    Prometheus exposition spec.
    """
    from tgwatcher.web.metrics import collect_metrics

    return Response(collect_metrics(), mimetype="text/plain; version=0.0.4")


# ── PEP 562 module-level forwarding for the 11 AppState globals ─────────
# Reads/writes of these names from external code (tests/test_metrics.py,
# tests/test_signals_export_phase1b.py, tgwatcher/web/metrics.py) are
# forwarded to _app_state so existing call sites work unchanged. Writes
# to names NOT in this set fall through to normal module attribute
# assignment (so _sse_bus, _listener_daemon, _auto_poll_state, etc.
# remain real module attributes).
_APP_STATE_FORWARDED = frozenset({
    "_storage", "_crawl_service", "_config", "_async_loop", "_auth_token",
    "_signal_engine", "_webhook_dispatcher", "_tg_client", "_tg_lock",
    "_signal_service", "_source_quality_tracker",
})


def __getattr__(name: str):
    if name in _APP_STATE_FORWARDED:
        return getattr(_app_state, name[1:])  # strip leading underscore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __setattr__(name: str, value) -> None:
    if name in _APP_STATE_FORWARDED:
        setattr(_app_state, name[1:], value)
    else:
        # Fall back to normal module attribute assignment. PEP 562 does not
        # provide a "default" path, so we manipulate the module dict directly.
        _sys.modules[__name__].__dict__[name] = value



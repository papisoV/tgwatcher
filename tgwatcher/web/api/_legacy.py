"""Flask REST API blueprint for TGWatcher — thin registration shim (Phase 2C).

All routes live in `routes_*.py` sub-modules. This module owns the four
singletons (`_app_state`, `_sse_bus`, `_auto_poll_daemon`,
`_listener_daemon`) and the `api` Blueprint, and re-exports helpers from
`._helpers` + proxies from `._proxies` so every `from ._legacy import X`
in `routes_*.py` keeps working (zero diff to routes_*).

PEP 562 `__getattr__` / `__setattr__` at the bottom forward reads/writes
of the 11 `_APP_STATE_FORWARDED` names to `_app_state` so existing call
sites and tests (`api_mod._storage = None`) work unchanged.
"""
import sys as _sys

from flask import Blueprint

from tgwatcher.client import TGClient  # noqa: F401  (historical re-export)
from tgwatcher.web.app_state import AppState
from tgwatcher.web.auto_poll_daemon import AutoPollDaemon
from tgwatcher.web.listener_daemon import ListenerDaemon
from tgwatcher.web.sse_bus import SSEBus

# Helpers — pure functions, small state, decorator, context-manager.
from ._helpers import (  # noqa: F401
    _atomic_write_config,
    _check_rate_limit,
    _disconnect_tg_client,
    _extract_auth_token,
    _get_auth_token_path,
    _get_listen_groups,
    _get_tg_client,
    _iso_z,
    _rate_limit_store,
    _run_coro,
    _tg_client_guard,
    require_auth,
)

# Proxy classes — instantiated below as singletons.
from ._proxies import (  # noqa: F401
    _AutoPollLockProxy,
    _AutoPollShutdownProxy,
    _AutoPollStateProxy,
    _AutoPollStopRequestedProxy,
)

api = Blueprint("api", __name__, url_prefix="/api")

# ── Singletons ─────────────────────────────────────────────────────────
# ListenerDaemon: encapsulates listener thread, stop_event, running flag,
# lock (Phase 2A). Late-binding host ref lets the daemon read this
# module's state (_tg_client_guard, _get_tg_client, _storage,
# _signal_engine, push_sse_event, push_new_message) at call time — avoids
# circular import.
_listener_daemon = ListenerDaemon()
_listener_daemon.bind_host(lambda: _sys.modules[__name__])

_sse_bus = SSEBus()                      # listeners, buffer, lock, event-id counter
_app_state = AppState()                  # the 11 cross-domain globals (Phase 2A full)
_auto_poll_daemon = AutoPollDaemon()     # per-group state, lock, shutdown/stop Events

# Proxy singletons — order matters: lock must exist before state proxy's
# __init__ reads it from this module's __dict__.
_auto_poll_shutdown = _AutoPollShutdownProxy()
_auto_poll_stop_requested = _AutoPollStopRequestedProxy()
_auto_poll_lock = _AutoPollLockProxy()
_auto_poll_state = _AutoPollStateProxy()


# ── Thin delegation wrappers (backward-compat for existing imports) ────
def init_services(config, async_loop=None) -> None:
    _app_state.init_services(config, async_loop=async_loop)


def _init_auto_poll(config: dict) -> None:
    _auto_poll_daemon.init_from_config(config)


def _auto_poll_loop() -> None:
    _auto_poll_daemon.run_loop()


def _load_or_create_auth_token() -> str:
    return _app_state.load_or_create_auth_token()


def _init_listener(config: dict) -> None:
    _listener_daemon.init_from_config(config)


def _start_listener_thread(listen_groups: list[dict]) -> bool:
    return _listener_daemon.start_thread(listen_groups)


async def _run_listener_async(listen_groups: list[dict]) -> None:
    await _listener_daemon.run_async(listen_groups)


def _stop_listener() -> bool:
    return _listener_daemon.stop()


def _init_signal_engine(config: dict) -> None:
    _app_state.init_signal_engine(config)


def push_sse_event(event_type: str, data: dict) -> None:
    _sse_bus.push(event_type, data)


def push_new_message(msg: dict) -> None:
    _sse_bus.push("new_messages", msg)


# ── PEP 562 forwarding for the 11 AppState globals ─────────────────────
# Reads/writes of these names from external code (tests/test_metrics.py,
# tests/test_signals_export_phase1b.py, web/metrics.py) are forwarded to
# _app_state. Writes to names NOT in this set fall through to normal
# module attribute assignment (so _sse_bus, _listener_daemon,
# _auto_poll_state, etc. remain real module attributes).
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
        # PEP 562 has no "default" path — manipulate the module dict directly.
        _sys.modules[__name__].__dict__[name] = value

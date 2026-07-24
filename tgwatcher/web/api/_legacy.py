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


@api.route("/messages", methods=["GET"])
@require_auth
def get_messages():
    chat_id = request.args.get("chat_id", type=int)
    keyword = request.args.get("keyword", type=str)
    sender_id = request.args.get("sender_id", type=int)
    date_from = request.args.get("date_from", type=str)
    date_to = request.args.get("date_to", type=str)
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("size", 50, type=int)
    media_type = request.args.get("media_type", type=str)
    include_deleted = request.args.get("include_deleted", "0") == "1"

    if keyword and len(keyword) > 200:
        return jsonify({"error": "Keyword too long (max 200 characters)"}), 400
    page_size = max(1, min(page_size, 200))

    df = local_to_utc(datetime.fromisoformat(date_from)) if date_from else None
    dt = None
    if date_to:
        dt_local = datetime.fromisoformat(date_to)
        if dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0:
            dt_local = dt_local.replace(hour=23, minute=59, second=59, microsecond=999999)
        dt = local_to_utc(dt_local)

    result = _storage.query_messages(
        chat_id=chat_id, keyword=keyword, sender_id=sender_id,
        date_from=df, date_to=dt,
        page=page, page_size=page_size,
        media_type=media_type, include_deleted=include_deleted,
    )
    return jsonify(result)


@api.route("/chats", methods=["GET"])
@require_auth
def get_chats():
    chats = _storage.get_chats()
    # Merge auto_catchup and auto_listen flags from config
    group_map = {g.get("id"): g for g in _config.get("groups", [])}
    for c in chats:
        g = group_map.get(c["chat_id"], {})
        c["auto_catchup"] = g.get("auto_catchup", False)
        c["auto_listen"] = g.get("auto_listen", False)
    return jsonify(chats)


@api.route("/senders", methods=["GET"])
@require_auth
def get_senders():
    chat_id = request.args.get("chat_id", type=int)
    senders = _storage.get_senders(chat_id=chat_id)
    return jsonify(senders)


@api.route("/messages/<int:message_id>/reply", methods=["GET"])
@require_auth
def get_reply_message(message_id):
    msg = _storage.get_message_by_id(message_id)
    if not msg:
        return jsonify({"error": "Message not found"}), 404
    return jsonify(msg)


@api.route("/messages/export", methods=["GET"])
@require_auth
def export_messages():
    fmt = request.args.get("format", "json")
    chat_id = request.args.get("chat_id", type=int)
    keyword = request.args.get("keyword", type=str)
    sender_id = request.args.get("sender_id", type=int)
    date_from = request.args.get("date_from", type=str)
    date_to = request.args.get("date_to", type=str)

    if keyword and len(keyword) > 200:
        return jsonify({"error": "Keyword too long"}), 400

    df = local_to_utc(datetime.fromisoformat(date_from)) if date_from else None
    dt = None
    if date_to:
        dt_local = datetime.fromisoformat(date_to)
        if dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0:
            dt_local = dt_local.replace(hour=23, minute=59, second=59, microsecond=999999)
        dt = local_to_utc(dt_local)

    result = _storage.query_messages(
        chat_id=chat_id, keyword=keyword, sender_id=sender_id,
        date_from=df, date_to=dt, page=1, page_size=50000,
    )
    messages = result.get("messages", [])

    if fmt == "csv":
        import csv
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["message_id", "chat_id", "chat_title", "sender_id", "sender_name",
                         "sender_username", "text", "reply_to_msg_id", "forward_from", "date",
                         "has_media", "is_edited", "edited_at", "media_type"])
        for m in messages:
            writer.writerow([m.get("message_id"), m.get("chat_id"), m.get("chat_title"),
                             m.get("sender_id"), m.get("sender_name"), m.get("sender_username"),
                             m.get("text", "").replace("\n", "\\n"), m.get("reply_to_msg_id"),
                             m.get("forward_from"), m.get("date"), m.get("has_media"),
                             m.get("is_edited"), m.get("edited_at"), m.get("media_type")])
        return Response(output.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=tgwatcher_export.csv"})

    if fmt == "markdown":
        lines = ["# 聊天记录导出\n"]
        meta_parts = []
        if chat_id:
            chat_title = messages[0].get("chat_title", "") if messages else ""
            meta_parts.append(f"群组: {chat_title}" if chat_title else f"群组ID: {chat_id}")
        else:
            meta_parts.append("群组: 全部")
        if date_from:
            meta_parts.append(f"起始: {date_from}")
        if date_to:
            meta_parts.append(f"结束: {date_to}")
        meta_parts.append(f"共 {len(messages)} 条消息")
        lines.append(" | ".join(meta_parts))
        lines.append("")
        for m in messages:
            date_str = (m.get("date") or "-").replace("T", " ")[:16]
            sender = m.get("sender_name") or m.get("sender_username") or "未知"
            chat_tag = f" [{m.get('chat_title', '')}]" if not chat_id and m.get("chat_title") else ""
            lines.append(f"### [{date_str}] {sender}{chat_tag}")
            text = m.get("text") or ""
            if text:
                lines.append(text)
            fwd = m.get("forward_from")
            if fwd:
                lines.append(f"*转发自: {fwd}*")
            reply = m.get("reply_to_msg_id")
            if reply:
                lines.append(f"*回复消息ID: {reply}*")
            lines.append("")
            lines.append("---")
            lines.append("")
        return Response("\n".join(lines), mimetype="text/markdown",
                        headers={"Content-Disposition": "attachment; filename=tgwatcher_export.md"})

    return jsonify(messages)


@api.route("/signals/export", methods=["GET"])
@require_auth
def export_signals():
    """Export signal analysis results with message context."""
    fmt = request.args.get("format", "json")
    chat_id = request.args.get("chat_id", type=int)
    date_from = request.args.get("date_from", type=str)
    date_to = request.args.get("date_to", type=str)
    event_type = request.args.get("event_type", type=str)
    direction = request.args.get("direction", type=str)
    llm_model = request.args.get("llm_model", type=str)
    is_signal = request.args.get("is_signal", type=str)
    count_only = request.args.get("count_only", "").lower() == "true"

    df = local_to_utc(datetime.fromisoformat(date_from)) if date_from else None
    dt = None
    if date_to:
        dt_local = datetime.fromisoformat(date_to)
        if dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0:
            dt_local = dt_local.replace(hour=23, minute=59, second=59, microsecond=999999)
        dt = local_to_utc(dt_local)

    rows = _storage.query_signals_export(
        chat_id=chat_id,
        date_from=df,
        date_to=dt,
        event_type=event_type,
        direction=direction,
        llm_model=llm_model,
        is_signal=is_signal,
        count_only=count_only,
    )

    if count_only:
        return jsonify({"count": rows})

    # Serialize date field for JSON/CSV/Markdown rendering
    for r in rows:
        d = r["date"]
        r["date"] = _iso_z(d) if isinstance(d, datetime) else (str(d) if d else None)

    if fmt == "csv":
        import csv
        import io
        output = io.StringIO()
        columns = ["message_id", "chat_id", "chat_title", "sender_name", "date", "text",
                    "direction", "magnitude", "urgency", "confidence",
                    "halflife_min", "symbols", "event_type", "reasoning"]
        writer = csv.writer(output)
        writer.writerow(columns)
        for r in rows:
            writer.writerow([
                r["message_id"], r["chat_id"], r["chat_title"], r["sender_name"],
                r["date"], (r["text"] or "").replace("\n", " "),
                r["direction"], r["magnitude"], r["urgency"], r["confidence"],
                r["halflife_min"], ",".join(r.get("symbols", [])),
                r["event_type"], r["reasoning"],
            ])
        return Response(output.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=signals_export.csv"})

    if fmt == "markdown":
        lines = ["# 信号分析导出\n"]
        meta_parts = []
        if chat_id:
            chat_title = rows[0].get("chat_title", "") if rows else ""
            meta_parts.append(f"群组: {chat_title}" if chat_title else f"群组ID: {chat_id}")
        else:
            meta_parts.append("群组: 全部")
        if date_from:
            meta_parts.append(f"起始: {date_from}")
        if date_to:
            meta_parts.append(f"结束: {date_to}")
        meta_parts.append(f"共 {len(rows)} 条")
        lines.append(" | ".join(meta_parts))
        lines.append("")

        for r in rows:
            date_str = (r.get("date") or "-").replace("T", " ")[:16]
            sender = r.get("sender_name") or "未知"
            chat_tag = f" [{r.get('chat_title', '')}]" if not chat_id and r.get("chat_title") else ""
            lines.append(f"### [{date_str}] {sender}{chat_tag}")

            text = r.get("text") or ""
            if text:
                lines.append(f"> {text}")
            lines.append("")

            d = r.get("direction", 0)
            direction_label = "利多" if d > 0.1 else ("利空" if d < -0.1 else "中性")
            symbols_str = ",".join(r.get("symbols", [])) or "-"
            event_map = {"security": "安全", "regulatory": "监管", "macro": "宏观",
                         "whale": "鲸鱼", "market": "市场", "listing": "上线",
                         "partnership": "合作", "other": "其他"}
            et = event_map.get(r["event_type"], r["event_type"])
            lines.append(f"**{direction_label}** ({d:+.2f}) | {et} | 幅度{r['magnitude']:.2f} | "
                         f"紧急{r['urgency']:.2f} | 置信{r['confidence']:.2f} | "
                         f"半衰期{r['halflife_min']}min | {symbols_str}")
            if r["reasoning"]:
                lines.append(f"  推理: {r['reasoning']}")

            lines.append("")
            lines.append("---")
            lines.append("")
        return Response("\n".join(lines), mimetype="text/markdown",
                        headers={"Content-Disposition": "attachment; filename=signals_export.md"})

    if fmt == "sqlite":
        import tempfile
        from sqlalchemy import create_engine as ce
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp_path = tmp.name
        tmp.close()
        eng = ce(f"sqlite:///{tmp_path}")
        with eng.connect() as c:
            c.execute(sql_text("PRAGMA journal_mode=WAL"))
            c.execute(sql_text("""
                CREATE TABLE IF NOT EXISTS tg_factors (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg_id       INTEGER NOT NULL,
                    ts           TEXT NOT NULL,
                    symbols      TEXT NOT NULL,
                    direction    REAL NOT NULL,
                    magnitude    REAL NOT NULL,
                    urgency      REAL NOT NULL,
                    confidence   REAL NOT NULL,
                    halflife_min INTEGER NOT NULL,
                    event_type   TEXT NOT NULL,
                    reasoning    TEXT NOT NULL,
                    created_at   TEXT DEFAULT (datetime('now')),
                    UNIQUE(msg_id)
                )
            """))
            c.execute(sql_text("CREATE INDEX IF NOT EXISTS idx_tg_factors_ts ON tg_factors(ts)"))
            c.execute(sql_text("CREATE INDEX IF NOT EXISTS idx_tg_factors_event_type ON tg_factors(event_type)"))
            for r in rows:
                sym_list = r.get("symbols", [])
                if not sym_list:
                    sym_list = ["*"]
                symbols_json = json.dumps(sym_list, ensure_ascii=False)
                ts_val = r.get("date", "")
                c.execute(sql_text("""
                    INSERT OR REPLACE INTO tg_factors
                    (msg_id, ts, symbols, direction, magnitude, urgency, confidence, halflife_min, event_type, reasoning)
                    VALUES (:msg_id, :ts, :symbols, :direction, :magnitude, :urgency, :confidence, :halflife_min, :event_type, :reasoning)
                """), {
                    "msg_id": r["message_id"],
                    "ts": ts_val,
                    "symbols": symbols_json,
                    "direction": r.get("direction", 0.0) or 0.0,
                    "magnitude": r.get("magnitude", 0.1) or 0.1,
                    "urgency": r.get("urgency", 0.1) or 0.1,
                    "confidence": r.get("confidence", 0.9) or 0.9,
                    "halflife_min": r.get("halflife_min", 60) or 60,
                    "event_type": r.get("event_type", "other") or "other",
                    "reasoning": r.get("reasoning", "") or "",
                })
            c.commit()
        eng.dispose()
        with open(tmp_path, "rb") as f:
            db_bytes = f.read()
        os.unlink(tmp_path)
        return Response(db_bytes, mimetype="application/x-sqlite3",
                        headers={"Content-Disposition": "attachment; filename=tg_factors.db"})

    return jsonify(rows)


@api.route("/config/groups/<int:chat_id>", methods=["DELETE"])
@require_auth
def delete_group(chat_id):
    # Remove from config if present
    groups = _config.get("groups") or []
    new_groups = [g for g in groups if g.get("id") != chat_id]
    config_changed = len(new_groups) < len(groups)
    if config_changed:
        _config["groups"] = new_groups
        config_path = os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))
        _atomic_write_config(_config, config_path)
    # Always delete database data regardless of config state
    deleted = _storage.delete_chat_data(chat_id)
    if not config_changed and deleted == 0:
        return jsonify({"error": "Group not found"}), 404
    return jsonify({"status": "removed", "groups": _config["groups"], "messages_deleted": deleted})


@api.route("/data/purge", methods=["POST"])
@require_auth
def purge_all_data():
    deleted = _storage.delete_all_data()
    return jsonify({"status": "purged", "messages_deleted": deleted})


# --- Crawl Control APIs ---

@api.route("/crawl/start", methods=["POST"])
@require_auth
def start_crawl():
    data = request.get_json(silent=True) or {}
    mode = data.get("mode", "incremental")
    if mode not in ("incremental", "full", "date_range", "catchup"):
        return jsonify({"error": "Invalid mode. Use: incremental, full, date_range, catchup"}), 400
    extra: dict = {}
    if mode == "date_range":
        offset_date = data.get("offset_date")
        until_date = data.get("until_date")
        if not offset_date or not until_date:
            return jsonify({"error": "date_range mode requires offset_date and until_date"}), 400
        extra["offset_date"] = offset_date
        extra["until_date"] = until_date
    ok = _crawl_service.start(mode=mode, **extra)
    if not ok:
        if mode == "catchup" and not [g for g in _config.get("groups", []) if g.get("auto_catchup", False)]:
            return jsonify({"error": "没有启用自动补爬的群组，请先在群组页面开启"}), 400
        return jsonify({"error": "Crawl already running"}), 409
    return jsonify({"status": "started", "mode": mode})


@api.route("/crawl/stop", methods=["POST"])
@require_auth
def stop_crawl():
    ok = _crawl_service.stop()
    if not ok:
        return jsonify({"error": "No crawl running"}), 409
    # Signal auto-poll daemon to suspend triggering new crawls. Without this,
    # the daemon would fire the next tick (often within seconds) and restart
    # a crawl immediately after the current one stops — making the user's
    # "stop" feel ignored. Cleared when auto-poll is re-enabled via
    # update_auto_poll() or on fresh startup via _init_auto_poll().
    _auto_poll_stop_requested.set()
    logger.info("Stop requested by user — auto-poll daemon suspended until re-enabled")
    return jsonify({"status": "stopping"})


@api.route("/crawl/status", methods=["GET"])
@require_auth
def crawl_status():
    return jsonify(_crawl_service.status)


@api.route("/crawl/auto-poll", methods=["GET"])
@require_auth
def get_auto_poll():
    """Return per-group auto-poll state with countdown to next tick."""
    now = time.time()
    with _auto_poll_lock:
        result = []
        for cid, s in _auto_poll_state.items():
            result.append({
                "chat_id": cid,
                "name": s.get("name", str(cid)),
                "enabled": s["enabled"],
                "interval_seconds": s["interval"],
                "remaining_seconds": max(0, int(s["next_tick_at"] - now)) if s["enabled"] else None,
            })
    return jsonify(result)


@api.route("/crawl/auto-poll/<int:chat_id>", methods=["PATCH"])
@require_auth
def update_auto_poll(chat_id: int):
    """Update per-group auto_poll settings and persist to config.yaml."""
    data = request.get_json(silent=True) or {}
    enabled = data.get("enabled")
    interval = data.get("interval_seconds")
    if enabled is None and interval is None:
        return jsonify({"error": "Provide 'enabled' and/or 'interval_seconds'"}), 400

    # Update in-memory config
    found = False
    for g in _config.get("groups", []):
        if g.get("id") == chat_id:
            found = True
            if enabled is not None:
                g["auto_poll"] = bool(enabled)
            if interval is not None:
                try:
                    iv = int(interval)
                except (TypeError, ValueError):
                    return jsonify({"error": "interval_seconds must be int"}), 400
                if iv < 5 or iv > 3600:
                    return jsonify({"error": "interval_seconds must be 5-3600"}), 400
                g["poll_interval_seconds"] = iv
            break
    if not found:
        return jsonify({"error": "Group not found in config"}), 404

    # Persist
    config_path = os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))
    _atomic_write_config(_config, config_path)

    # Update live state
    with _auto_poll_lock:
        s = _auto_poll_state.get(chat_id)
        if s is None:
            s = {"name": str(chat_id)}
            _auto_poll_state[chat_id] = s
        if enabled is not None:
            s["enabled"] = bool(enabled)
        if interval is not None:
            s["interval"] = iv
        s["next_tick_at"] = time.time() + s["interval"]
        s["name"] = next((g.get("name", str(chat_id)) for g in _config["groups"] if g.get("id") == chat_id), str(chat_id))

    # Re-enabling auto-poll clears any user-initiated stop signal so the
    # daemon resumes triggering ticks. This is the only path that clears
    # _auto_poll_stop_requested besides a fresh startup (_init_auto_poll).
    if enabled is True:
        _auto_poll_stop_requested.clear()
        logger.info("Auto-poll re-enabled by user — stop signal cleared, daemon resumed")

    push_sse_event("auto_poll_tick", {
        "chat_id": chat_id,
        "name": s.get("name"),
        "next_tick_at": s["next_tick_at"],
        "interval": s["interval"],
        "enabled": s["enabled"],
    })
    return jsonify({"status": "updated", "chat_id": chat_id,
                    "enabled": s["enabled"], "interval_seconds": s["interval"]})


# --- Config APIs ---

@api.route("/config", methods=["GET"])
@require_auth
def get_config():
    safe_config = {}
    safe_config["groups"] = _config.get("groups", [])
    safe_config["crawl"] = _config.get("crawl", {})
    safe_config["proxy"] = {"enabled": _config.get("proxy", {}).get("enabled", False)}
    safe_config["storage"] = _config.get("storage", {})
    safe_config["telegram"] = {
        "phone": _config["telegram"]["phone"],
        "session_dir": _config["telegram"].get("session_dir", "./sessions"),
    }
    safe_config["web"] = _config.get("web", {})
    safe_config["catchup"] = _config.get("catchup", {"enabled": True, "limit": 1000})
    return jsonify(safe_config)


@api.route("/config/groups", methods=["PUT"])
@require_auth
def update_groups():
    data = request.get_json(silent=True)
    if not data or "groups" not in data:
        return jsonify({"error": "Missing 'groups' in body"}), 400
    for g in data["groups"]:
        if not g.get("id") and not g.get("username"):
            return jsonify({"error": "Each group must have 'id' or 'username'"}), 400
    # Preserve per-group flags from existing config for groups that still exist
    existing_map = {g.get("id"): g for g in _config.get("groups", [])}
    for g in data["groups"]:
        gid = g.get("id")
        if gid in existing_map:
            ex = existing_map[gid]
            g.setdefault("auto_catchup", ex.get("auto_catchup", False))
            g.setdefault("auto_poll", ex.get("auto_poll", False))
            g.setdefault("poll_interval_seconds", ex.get("poll_interval_seconds", 15))
            g.setdefault("auto_listen", ex.get("auto_listen", False))
    _config["groups"] = data["groups"]
    config_path = os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))
    _atomic_write_config(_config, config_path)
    return jsonify({"status": "updated", "groups": _config["groups"]})


@api.route("/config/groups/<int:chat_id>/auto_catchup", methods=["PATCH"])
@require_auth
def toggle_group_auto_catchup(chat_id):
    data = request.get_json(silent=True) or {}
    auto_catchup = data.get("auto_catchup")
    if auto_catchup is None:
        return jsonify({"error": "Missing 'auto_catchup' in body"}), 400
    groups = _config.get("groups", [])
    found = False
    for g in groups:
        if g.get("id") == chat_id:
            g["auto_catchup"] = bool(auto_catchup)
            found = True
            break
    if not found:
        return jsonify({"error": "Group not found in config"}), 404
    config_path = os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))
    _atomic_write_config(_config, config_path)
    return jsonify({"status": "updated", "chat_id": chat_id, "auto_catchup": bool(auto_catchup)})


@api.route("/config/groups/<int:chat_id>/auto_listen", methods=["PATCH"])
@require_auth
def toggle_group_auto_listen(chat_id: int):
    """Toggle per-group auto_listen. If turning on and listener not running, start it.
    If turning off and no groups remain, stop the listener."""
    data = request.get_json(silent=True) or {}
    auto_listen = data.get("auto_listen")
    if auto_listen is None:
        return jsonify({"error": "Missing 'auto_listen' in body"}), 400
    groups = _config.get("groups", [])
    found = False
    for g in groups:
        if g.get("id") == chat_id:
            g["auto_listen"] = bool(auto_listen)
            found = True
            break
    if not found:
        return jsonify({"error": "Group not found in config"}), 404
    config_path = os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))
    _atomic_write_config(_config, config_path)

    # Side-effect: start/stop listener based on remaining auto_listen groups
    listen_groups = _get_listen_groups(_config)
    if auto_listen and not _listener_daemon.is_running and listen_groups:
        _start_listener_thread(listen_groups)
    elif not auto_listen and not listen_groups and _listener_daemon.is_running:
        _stop_listener()

    return jsonify({"status": "updated", "chat_id": chat_id,
                    "auto_listen": bool(auto_listen),
                    "listener_running": _listener_daemon.is_running})


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

@api.route("/signal/process", methods=["POST"])
@require_auth
def signal_process():
    """Start batch signal processing."""
    if not _signal_service:
        return jsonify({"error": "Signal processing not enabled"}), 400
    body = request.get_json(silent=True) or {}
    chat_id = body.get("chat_id")
    overwrite = body.get("overwrite", False)
    started = _signal_service.start(chat_id=chat_id, overwrite=overwrite)
    if not started:
        return jsonify({"error": "Signal processing already running"}), 409
    return jsonify({"status": "started"})


@api.route("/signal/process/status", methods=["GET"])
@require_auth
def signal_process_status():
    """Get batch signal processing status."""
    if not _signal_service:
        return jsonify({"error": "Signal processing not enabled"}), 400
    return jsonify(_signal_service.status)


@api.route("/signal/process/stop", methods=["POST"])
@require_auth
def signal_process_stop():
    """Stop batch signal processing."""
    if not _signal_service:
        return jsonify({"error": "Signal processing not enabled"}), 400
    stopped = _signal_service.stop()
    return jsonify({"stopped": stopped})


@api.route("/signal/factors", methods=["GET"])
@require_auth
def signal_factors():
    """Query signal factors with filters."""
    if not _storage:
        return jsonify({"error": "Storage not initialized"}), 500
    chat_id = request.args.get("chat_id", type=int)
    event_type = request.args.get("event_type")
    direction = request.args.get("direction")
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("page_size", 50, type=int)
    if event_type and event_type not in ("security", "regulatory", "macro", "whale", "market", "listing", "partnership", "other"):
        return jsonify({"error": "Invalid event_type value"}), 400
    if direction and direction not in ("bullish", "neutral", "bearish"):
        return jsonify({"error": "Invalid direction value"}), 400
    # Convert local date strings to UTC for querying against UTC-stored Message.date
    date_from_utc = None
    date_to_utc = None
    if date_from:
        date_from_utc = local_to_utc(datetime.fromisoformat(date_from))
    if date_to:
        dt_local = datetime.fromisoformat(date_to)
        if dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0:
            dt_local = dt_local.replace(hour=23, minute=59, second=59, microsecond=999999)
        date_to_utc = local_to_utc(dt_local)
    result = _storage.query_signal_factors(
        chat_id=chat_id, event_type=event_type, direction=direction,
        date_from=date_from_utc, date_to=date_to_utc,
        page=page, page_size=page_size,
    )
    return jsonify(result)


@api.route("/signal/stats", methods=["GET"])
@require_auth
def signal_stats():
    """Get aggregated signal factor statistics."""
    if not _storage:
        return jsonify({"error": "Storage not initialized"}), 500
    chat_id = request.args.get("chat_id", type=int)
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")
    # Convert local date strings to UTC
    date_from_utc = None
    date_to_utc = None
    if date_from:
        date_from_utc = local_to_utc(datetime.fromisoformat(date_from)).isoformat()
    if date_to:
        dt_local = datetime.fromisoformat(date_to)
        if dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0:
            dt_local = dt_local.replace(hour=23, minute=59, second=59, microsecond=999999)
        date_to_utc = local_to_utc(dt_local).isoformat()
    result = _storage.get_signal_stats(chat_id=chat_id, date_from=date_from_utc, date_to=date_to_utc)
    return jsonify(result)


@api.route("/signal/trend", methods=["GET"])
@require_auth
def signal_trend():
    """Get sentiment trend time series."""
    if not _storage:
        return jsonify({"error": "Storage not initialized"}), 500
    period = request.args.get("period", "day")
    days = request.args.get("days", 30, type=int)
    chat_id = request.args.get("chat_id", type=int)
    result = _storage.get_signal_trend(period=period, days=days, chat_id=chat_id)
    return jsonify(result)


@api.route("/signal/config", methods=["GET"])
@require_auth
def signal_config_get():
    """Get signal configuration (safe fields only)."""
    signal_cfg = _config.get("signal", {}) if _config else {}
    safe_cfg = {
        "enabled": signal_cfg.get("enabled", False),
        "batch_size": signal_cfg.get("batch_size", 50),
        "llm_delay": signal_cfg.get("llm_delay", 1.0),
        "factor_version": signal_cfg.get("factor_version", 1),
        "filter": signal_cfg.get("filter", {}),
        "llm": {
            "provider": signal_cfg.get("llm", {}).get("provider", ""),
            "model": signal_cfg.get("llm", {}).get("model", ""),
            "base_url": signal_cfg.get("llm", {}).get("base_url", ""),
        },
    }
    return jsonify(safe_cfg)


@api.route("/signal/config", methods=["PUT"])
@require_auth
def signal_config_update():
    """Update signal keywords configuration."""
    if not _config:
        return jsonify({"error": "Config not loaded"}), 500
    body = request.get_json(silent=True) or {}
    keywords = body.get("keywords")
    if keywords and isinstance(keywords, dict):
        if "signal" not in _config:
            _config["signal"] = {}
        _config["signal"]["keywords"] = keywords
        return jsonify({"status": "updated"})
    return jsonify({"error": "Invalid keywords format"}), 400


@api.route("/signal/reprocess/<int:message_id>", methods=["POST"])
@require_auth
def signal_reprocess(message_id: int):
    """Re-process a single message's signal factors."""
    if not _signal_engine or not _storage:
        return jsonify({"error": "Signal processing not available"}), 400
    chat_id = request.args.get("chat_id", type=int)
    if not chat_id:
        return jsonify({"error": "chat_id required"}), 400
    # Find the message
    msg = _storage.get_message_by_id(message_id)
    if not msg:
        return jsonify({"error": "Message not found"}), 404
    factor = _signal_engine.process_message(msg)
    if factor:
        return jsonify(factor)
    return jsonify({"error": "Processing failed"}), 500


# ===== Signal outcome feedback (downstream reports actual price action) =====

@api.route("/signals/<int:message_id>/outcome", methods=["POST"])
@require_auth
def record_signal_outcome(message_id: int):
    """Record a downstream-reported outcome for a signal.

    Body may include chat_id; if omitted, falls back to ?chat_id= query param.
    Required: chat_id. Optional: actual_direction, magnitude_pct, time_horizon_min,
    price_t0, price_tn, note, source.
    """
    if not _storage:
        return jsonify({"error": "Storage not initialized"}), 500
    data = request.get_json(silent=True) or {}
    chat_id = data.get("chat_id") or request.args.get("chat_id", type=int)
    if not chat_id:
        return jsonify({"error": "chat_id required (body or query)"}), 400
    outcome = {
        "message_id": message_id,
        "chat_id": int(chat_id),
        "actual_direction": data.get("actual_direction"),
        "magnitude_pct": data.get("magnitude_pct"),
        "time_horizon_min": data.get("time_horizon_min"),
        "price_t0": data.get("price_t0"),
        "price_tn": data.get("price_tn"),
        "note": data.get("note"),
        "source": data.get("source"),
    }
    try:
        saved = _storage.save_signal_outcome(outcome)
        # Serialize datetimes so jsonify doesn't choke on raw datetime objects.
        for k, v in list(saved.items()):
            iso = _iso_z(v)
            if iso is not None:
                saved[k] = iso
        # Accumulate into source quality tracker (skeleton — no-op effect
        # until outcomes actually flow in, but the wiring is in place).
        if _source_quality_tracker is not None:
            try:
                _source_quality_tracker.accumulate(saved)
            except Exception as qe:
                logger.warning("Source quality tracker accumulate failed: %s", qe)
        return jsonify({"status": "recorded", "outcome": saved})
    except Exception as e:
        logger.error("save_signal_outcome failed: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


@api.route("/signals/source-quality", methods=["GET"])
@require_auth
def get_source_quality():
    """Return per-chat source quality stats accumulated from outcome feedback.

    Skeleton endpoint: returns zero stats until outcomes flow in (Selene
    not yet integrated). Per-chat aggregation includes outcome_count,
    avg_magnitude_pct, direction_distribution, last_outcome_at.

    Optional ?chat_id=<id> filters to a single chat.
    """
    if _source_quality_tracker is None:
        return jsonify({"error": "Source quality tracker not initialized"}), 503
    chat_id = request.args.get("chat_id", type=int)
    if chat_id is not None:
        return jsonify(_source_quality_tracker.stats(chat_id=chat_id))
    return jsonify(_source_quality_tracker.to_dict())


@api.route("/signals/<int:message_id>/outcomes", methods=["GET"])
@require_auth
def get_signal_outcomes(message_id: int):
    """List all outcomes reported for a signal."""
    if not _storage:
        return jsonify({"error": "Storage not initialized"}), 500
    chat_id = request.args.get("chat_id", type=int)
    if not chat_id:
        return jsonify({"error": "chat_id required"}), 400
    outcomes = _storage.get_signal_outcomes(message_id, chat_id)
    # Serialize datetimes
    for o in outcomes:
        for k, v in list(o.items()):
            iso = _iso_z(v)
            if iso is not None:
                o[k] = iso
    return jsonify({"message_id": message_id, "chat_id": chat_id, "outcomes": outcomes})


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



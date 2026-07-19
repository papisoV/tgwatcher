"""Flask REST API for TGWatcher."""
import asyncio
import functools
import json
import logging
import os
import secrets
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from flask import Blueprint, jsonify, request, Response

from tgwatcher.storage import Storage
from tgwatcher.client import TGClient
from tgwatcher.web.crawl_service import CrawlService
from tgwatcher.signal_filter import KeywordFilter
from tgwatcher.signal_llm import SignalLLMClient
from tgwatcher.signal_engine import SignalEngine
from tgwatcher.web.signal_service import SignalService
from tgwatcher.tz_utils import local_to_utc

logger = logging.getLogger(__name__)

api = Blueprint("api", __name__, url_prefix="/api")

_storage: Storage | None = None
_crawl_service: CrawlService | None = None
_config: dict | None = None
_async_loop = None
_auth_token: str | None = None
_tg_client: TGClient | None = None
_tg_lock = threading.Lock()

# SSE event bus
_sse_listeners: list[threading.Event] = []
_sse_events: list[dict] = []
_sse_lock = threading.Lock()
_sse_event_id = 0

# Simple in-memory rate limiter for login endpoints
_rate_limit_store: dict[str, list[float]] = {}

_signal_service: SignalService | None = None
_signal_engine: SignalEngine | None = None

MAX_SSE_EVENTS = 200


def _get_auth_token_path() -> Path:
    config_dir = Path(os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))).parent
    return config_dir / ".tgwatcher_auth"


def _load_or_create_auth_token() -> str:
    global _auth_token
    token_path = _get_auth_token_path()
    if token_path.exists():
        _auth_token = token_path.read_text().strip()
        if _auth_token:
            return _auth_token
    _auth_token = secrets.token_hex(32)
    token_path.write_text(_auth_token)
    logger.info("=" * 60)
    logger.info("Generated new auth token (also saved to %s)", token_path)
    logger.info("Token: %s", _auth_token)
    logger.info("=" * 60)
    return _auth_token


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
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if not token:
            token = request.args.get("token", "")
        if token != _auth_token:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


def init_services(config, async_loop=None) -> None:
    global _storage, _crawl_service, _config, _async_loop, _signal_service, _signal_engine
    _config = config
    _async_loop = async_loop
    db_path = config["storage"]["db_path"]
    _storage = Storage(db_path)
    _storage.init_db()

    def _on_status_change(status: dict):
        push_sse_event("crawl_status", status)

    _crawl_service = CrawlService(
        config, async_loop=async_loop, storage=_storage, on_status_change=_on_status_change,
        get_tg_client=_get_tg_client, tg_lock=_tg_lock,
    )
    _load_or_create_auth_token()

    # Initialize signal engine if enabled
    signal_cfg = config.get("signal", {})
    if signal_cfg.get("enabled", False):
        _init_signal_engine(config)
    else:
        _signal_service = None
        _signal_engine = None

    # Auto-catchup on startup
    catchup_cfg = config.get("catchup", {})
    if catchup_cfg.get("enabled", True):
        catchup_groups = [g for g in config.get("groups", []) if g.get("auto_catchup", False)]
        if catchup_groups:
            def _delayed_catchup():
                import time
                time.sleep(5)
                logger.info("Auto-catchup: starting for %d groups", len(catchup_groups))
                _crawl_service.start(mode="catchup")
            threading.Thread(target=_delayed_catchup, daemon=True).start()

    # Auto-poll daemon — per-group periodic incremental crawl
    _init_auto_poll(config)
    threading.Thread(target=_auto_poll_loop, daemon=True, name="auto-poll").start()


# ── Auto-poll state ─────────────────────────────────────────────────────
_auto_poll_state: dict[int, dict] = {}  # {chat_id: {enabled, interval, next_tick_at, name}}
_auto_poll_lock = threading.Lock()


def _init_auto_poll(config: dict) -> None:
    """Populate _auto_poll_state from config (called at startup)."""
    with _auto_poll_lock:
        _auto_poll_state.clear()
        now = time.time()
        for g in config.get("groups", []):
            gid = g.get("id")
            if not gid:
                continue
            _auto_poll_state[gid] = {
                "enabled": bool(g.get("auto_poll", False)),
                "interval": int(g.get("poll_interval_seconds", 15)),
                "next_tick_at": now + int(g.get("poll_interval_seconds", 15)),
                "name": g.get("name", str(gid)),
            }


def _auto_poll_loop() -> None:
    """Daemon thread: every 1s, scan _auto_poll_state for due ticks and fire incremental crawl."""
    logger.info("Auto-poll daemon started")
    while True:
        try:
            time.sleep(1)
            now = time.time()
            with _auto_poll_lock:
                due = [
                    (cid, s) for cid, s in _auto_poll_state.items()
                    if s["enabled"] and now >= s["next_tick_at"]
                ]
                # Reschedule immediately so countdown doesn't drift
                for cid, s in due:
                    s["next_tick_at"] = now + s["interval"]
            if not due:
                continue
            # Skip if any crawl currently running
            if _crawl_service and _crawl_service.status.get("running"):
                continue
            # Trigger single-group incremental crawl for the most-due group
            cid, s = due[0]
            logger.info("Auto-poll: triggering incremental crawl for %s (%s)", s.get("name"), cid)
            try:
                if _crawl_service:
                    _crawl_service.start(mode="incremental", group_id=cid)
            except Exception as e:
                logger.warning("Auto-poll crawl start failed: %s", e)
            push_sse_event("auto_poll_tick", {
                "chat_id": cid,
                "name": s.get("name"),
                "next_tick_at": s["next_tick_at"],
                "interval": s["interval"],
            })
        except Exception as e:
            logger.warning("Auto-poll loop error: %s", e)


def _init_signal_engine(config: dict) -> None:
    """Initialize signal engine, LLM client, and service. Called from init_services."""
    global _signal_service, _signal_engine
    import os
    signal_cfg = config.get("signal", {})
    llm_cfg = signal_cfg.get("llm", {})

    # API key validation: env overrides config
    api_key = os.environ.get("SIGNAL_LLM_API_KEY") or llm_cfg.get("api_key", "")
    if not api_key:
        logger.warning("Signal enabled but no API key configured. Disabling signal processing.")
        signal_cfg["enabled"] = False
        _signal_service = None
        _signal_engine = None
        return

    keyword_filter = KeywordFilter(signal_cfg)
    llm_client = SignalLLMClient(llm_cfg)
    _signal_engine = SignalEngine(_storage, keyword_filter, llm_client, signal_cfg)

    def _on_signal_status(status: dict):
        push_sse_event("signal_process_status", status)

    _signal_service = SignalService(_signal_engine, signal_cfg, on_status_change=_on_signal_status)
    logger.info("Signal engine initialized (model=%s)", llm_cfg.get("model", "unknown"))


def push_sse_event(event_type: str, data: dict) -> None:
    global _sse_event_id
    with _sse_lock:
        _sse_event_id += 1
        event = {"id": _sse_event_id, "type": event_type, "data": data}
        _sse_events.append(event)
        if len(_sse_events) > MAX_SSE_EVENTS:
            del _sse_events[:MAX_SSE_EVENTS // 2]
        for listener in _sse_listeners:
            listener.set()


def push_new_message(msg: dict) -> None:
    push_sse_event("new_messages", msg)


def _get_tg_client() -> TGClient:
    """Get or create the shared TGClient singleton.

    All API endpoints must use this instead of creating their own TelegramClient,
    because Telethon's SQLite session file cannot be opened by multiple clients
    simultaneously (causes 'database is locked' errors).
    """
    global _tg_client
    if _tg_client is not None:
        return _tg_client
    loop = _async_loop.get_loop() if _async_loop else None
    _tg_client = TGClient(_config, loop=loop)
    return _tg_client


def _disconnect_tg_client() -> None:
    """Disconnect and discard the shared TGClient so the next call gets a fresh one."""
    global _tg_client
    if _tg_client is None:
        return
    try:
        if _async_loop:
            _async_loop.run_coroutine(_tg_client.disconnect())
        else:
            import asyncio
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_tg_client.disconnect())
            finally:
                loop.close()
    except Exception:
        pass
    _tg_client = None


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

@api.route("/stats", methods=["GET"])
@require_auth
def get_stats():
    stats = _storage.get_stats()
    stats["earliest_message"] = stats["earliest_message"].isoformat() if stats["earliest_message"] else None
    stats["latest_message"] = stats["latest_message"].isoformat() if stats["latest_message"] else None
    return jsonify(stats)


@api.route("/stats/trend", methods=["GET"])
@require_auth
def get_stats_trend():
    period = request.args.get("period", "day")
    days = request.args.get("days", 30, type=int)
    chat_id = request.args.get("chat_id", type=int)
    days = max(1, min(days, 365))
    result = _storage.get_message_trend(period=period, days=days, chat_id=chat_id)
    return jsonify(result)


@api.route("/stats/heatmap", methods=["GET"])
@require_auth
def get_stats_heatmap():
    chat_id = request.args.get("chat_id", type=int)
    result = _storage.get_activity_heatmap(chat_id=chat_id)
    return jsonify(result)


@api.route("/stats/comparison", methods=["GET"])
@require_auth
def get_stats_comparison():
    result = _storage.get_group_comparison()
    return jsonify(result)


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
    # Merge auto_catchup flag from config
    group_map = {g.get("id"): g.get("auto_catchup", False) for g in _config.get("groups", [])}
    for c in chats:
        c["auto_catchup"] = group_map.get(c["chat_id"], False)
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
    from sqlalchemy import text as sql_text

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

    rows = []
    with _storage.engine.connect() as conn:
        where_clauses = ["m.is_deleted = 0", "m.text IS NOT NULL", "f.llm_status = 'completed'"]
        params: dict = {}
        if chat_id:
            where_clauses.append("m.chat_id = :chat_id")
            params["chat_id"] = chat_id
        if df:
            where_clauses.append("m.date >= :df")
            params["df"] = df.isoformat()
        if dt:
            where_clauses.append("m.date <= :dt")
            params["dt"] = dt.isoformat()
        if event_type:
            where_clauses.append("f.event_type = :event_type")
            params["event_type"] = event_type
        if direction == "bullish":
            where_clauses.append("f.direction > 0")
        elif direction == "bearish":
            where_clauses.append("f.direction < 0")
        if llm_model:
            where_clauses.append("f.llm_model = :llm_model")
            params["llm_model"] = llm_model
        if is_signal == "true":
            where_clauses.append("f.is_signal = 1")
        elif is_signal == "false":
            where_clauses.append("f.is_signal = 0")

        where = " AND ".join(where_clauses)

        if count_only:
            count = conn.execute(sql_text(f"""
                SELECT COUNT(*) FROM messages m
                INNER JOIN signal_factors f ON m.message_id = f.message_id AND m.chat_id = f.chat_id
                WHERE {where}
            """), params).scalar()
            return jsonify({"count": count})

        query = f"""
            SELECT m.message_id, m.chat_id, m.chat_title, m.sender_name,
                   m.text, m.date,
                   f.direction, f.magnitude, f.urgency, f.confidence,
                   f.halflife_min, f.symbols, f.event_type, f.reasoning
            FROM messages m
            INNER JOIN signal_factors f ON m.message_id = f.message_id AND m.chat_id = f.chat_id
            WHERE {where}
            ORDER BY m.date DESC
        """
        for row in conn.execute(sql_text(query), params):
            rows.append({
                "message_id": row.message_id,
                "chat_id": row.chat_id,
                "chat_title": row.chat_title,
                "sender_name": row.sender_name,
                "text": row.text,
                "date": row.date.isoformat() if isinstance(row.date, datetime) else (str(row.date) if row.date else None),
                "direction": row.direction,
                "magnitude": row.magnitude,
                "urgency": row.urgency,
                "confidence": row.confidence,
                "halflife_min": row.halflife_min,
                "symbols": json.loads(row.symbols) if row.symbols else [],
                "event_type": row.event_type,
                "reasoning": row.reasoning,
            })

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
    # Preserve auto_catchup flags from existing config for groups that still exist
    existing_map = {g.get("id"): g.get("auto_catchup", False) for g in _config.get("groups", [])}
    for g in data["groups"]:
        gid = g.get("id")
        if gid in existing_map:
            g.setdefault("auto_catchup", existing_map[gid])
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
    token = request.args.get("token", "")
    if _auth_token and token != _auth_token:
        return jsonify({"error": "Unauthorized"}), 401

    listener_event = threading.Event()
    with _sse_lock:
        _sse_listeners.append(listener_event)
        last_id = _sse_event_id

    def generate():
        nonlocal last_id
        try:
            while True:
                listener_event.wait(timeout=30)
                listener_event.clear()
                with _sse_lock:
                    new_events = [e for e in _sse_events if e["id"] > last_id]
                    if new_events:
                        last_id = new_events[-1]["id"]
                for event in new_events:
                    yield f"id: {event['id']}\nevent: {event['type']}\ndata: {json.dumps(event['data'], default=str)}\n\n"
                # Keep event list bounded (secondary check)
                with _sse_lock:
                    if len(_sse_events) > MAX_SSE_EVENTS:
                        del _sse_events[:MAX_SSE_EVENTS // 2]
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                if listener_event in _sse_listeners:
                    _sse_listeners.remove(listener_event)

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


@api.route("/auth/bootstrap", methods=["GET"])
def auth_bootstrap():
    """Auto-login for localhost: returns the auth token so the browser can
    store it in localStorage, skipping the manual token entry step.

    Only responds to loopback / same-host requests — the token file already
    lives on the user's machine, so this just removes the copy-paste step.
    Remote requests get 403.
    """
    ip = request.remote_addr or "unknown"
    if not _check_rate_limit(f"auth_bootstrap:{ip}", max_requests=10, window=60):
        return jsonify({"error": "Rate limited"}), 429

    if _auth_token is None:
        return jsonify({"token": None})

    loopback = {"127.0.0.1", "::1", "localhost"}
    if ip not in loopback:
        return jsonify({"error": "Forbidden"}), 403

    return jsonify({"token": _auth_token})


@api.route("/login/status", methods=["GET"])
def login_status():
    ip = request.remote_addr or "unknown"
    if not _check_rate_limit(f"login_status:{ip}", max_requests=30, window=60):
        return jsonify({"error": "Rate limited"}), 429

    if _auth_token is not None:
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if not token:
            token = request.args.get("token", "")
        if token != _auth_token:
            return jsonify({"error": "Unauthorized"}), 401

    phone = _config["telegram"]["phone"]

    try:
        with _tg_client_guard() as tg:
            connected = _run_coro(tg.client.is_user_authorized())
    except Exception as e:
        logger.warning("Login status check failed: %s", e, exc_info=True)
        connected = False

    return jsonify({"logged_in": connected, "phone": phone})


@api.route("/login", methods=["POST"])
def do_login():
    ip = request.remote_addr or "unknown"
    if not _check_rate_limit(f"login:{ip}"):
        return jsonify({"error": "Rate limited"}), 429

    if _auth_token is not None:
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if token != _auth_token:
            return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    code = data.get("code")
    phone_code_hash = data.get("phone_code_hash")

    phone = _config["telegram"]["phone"]

    try:
        with _tg_client_guard() as tg:
            authorized = _run_coro(tg.client.is_user_authorized())

            if authorized:
                return jsonify({"status": "already_logged_in"})

            if code and phone_code_hash:
                try:
                    _run_coro(tg.client.sign_in(phone, code, phone_code_hash=phone_code_hash))
                    return jsonify({"status": "logged_in"})
                except Exception as e:
                    return jsonify({"error": str(e)}), 400
            else:
                try:
                    result = _run_coro(tg.client.send_code_request(phone))
                    return jsonify({"status": "code_sent", "phone_code_hash": result.phone_code_hash})
                except Exception as e:
                    return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.error("Login error: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


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

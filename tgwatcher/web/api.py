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

    df = datetime.fromisoformat(date_from) if date_from else None
    dt = datetime.fromisoformat(date_to) if date_to else None

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

    df = datetime.fromisoformat(date_from) if date_from else None
    dt = datetime.fromisoformat(date_to) if date_to else None

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

    return jsonify(messages)


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
    if mode not in ("incremental", "full", "date_range"):
        return jsonify({"error": "Invalid mode. Use: incremental, full, date_range"}), 400
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
    _config["groups"] = data["groups"]
    config_path = os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))
    _atomic_write_config(_config, config_path)
    return jsonify({"status": "updated", "groups": _config["groups"]})


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
    sentiment = request.args.get("sentiment")
    event_type = request.args.get("event_type")
    scope = request.args.get("scope")
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("page_size", 50, type=int)
    if sentiment and sentiment not in ("bullish", "neutral", "bearish"):
        return jsonify({"error": "Invalid sentiment value"}), 400
    if event_type and event_type not in ("regulatory", "macro", "exploit", "listing", "partnership", "governance", "market", "other"):
        return jsonify({"error": "Invalid event_type value"}), 400
    if scope and scope not in ("macro", "micro"):
        return jsonify({"error": "Invalid scope value"}), 400
    result = _storage.query_signal_factors(
        chat_id=chat_id, sentiment=sentiment, event_type=event_type,
        scope=scope, date_from=date_from, date_to=date_to,
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
    result = _storage.get_signal_stats(chat_id=chat_id, date_from=date_from, date_to=date_to)
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

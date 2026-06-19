"""Flask REST API for TGWatcher."""
import functools
import json
import logging
import os
import secrets
import threading
import time
from datetime import datetime
from pathlib import Path

from flask import Blueprint, jsonify, request, Response

from tgwatcher.storage import Storage
from tgwatcher.client import TGClient
from tgwatcher.web.crawl_service import CrawlService

logger = logging.getLogger(__name__)

api = Blueprint("api", __name__, url_prefix="/api")

_storage: Storage | None = None
_crawl_service: CrawlService | None = None
_config: dict | None = None
_async_loop = None
_auth_token: str | None = None

# SSE event bus
_sse_listeners: list[threading.Event] = []
_sse_events: list[dict] = []
_sse_lock = threading.Lock()
_sse_event_id = 0

# Simple in-memory rate limiter for login endpoints
_rate_limit_store: dict[str, list[float]] = {}


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
    logger.info("Generated new auth token. Token: %s...%s", _auth_token[:8], _auth_token[-4:])
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
    global _storage, _crawl_service, _config, _async_loop
    _config = config
    _async_loop = async_loop
    db_path = config["storage"]["db_path"]
    _storage = Storage(db_path)
    _storage.init_db()

    def _on_status_change(status: dict):
        push_sse_event("crawl_status", status)

    _crawl_service = CrawlService(config, async_loop=async_loop, on_status_change=_on_status_change)
    _load_or_create_auth_token()


def push_sse_event(event_type: str, data: dict) -> None:
    global _sse_event_id
    with _sse_lock:
        _sse_event_id += 1
        event = {"id": _sse_event_id, "type": event_type, "data": data}
        _sse_events.append(event)
        for listener in _sse_listeners:
            listener.set()


def push_new_message(msg: dict) -> None:
    push_sse_event("new_messages", msg)


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

    if keyword and len(keyword) > 200:
        return jsonify({"error": "Keyword too long (max 200 characters)"}), 400
    page_size = max(1, min(page_size, 200))

    df = datetime.fromisoformat(date_from) if date_from else None
    dt = datetime.fromisoformat(date_to) if date_to else None

    result = _storage.query_messages(
        chat_id=chat_id, keyword=keyword, sender_id=sender_id,
        date_from=df, date_to=dt,
        page=page, page_size=page_size,
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
                         "sender_username", "text", "reply_to_msg_id", "forward_from", "date", "has_media"])
        for m in messages:
            writer.writerow([m.get("message_id"), m.get("chat_id"), m.get("chat_title"),
                             m.get("sender_id"), m.get("sender_name"), m.get("sender_username"),
                             m.get("text", "").replace("\n", "\\n"), m.get("reply_to_msg_id"),
                             m.get("forward_from"), m.get("date"), m.get("has_media")])
        return Response(output.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=tgwatcher_export.csv"})

    return jsonify(messages)


@api.route("/config/groups/<int:chat_id>", methods=["DELETE"])
@require_auth
def delete_group(chat_id):
    groups = _config.get("groups") or []
    new_groups = [g for g in groups if g.get("id") != chat_id]
    if len(new_groups) == len(groups):
        return jsonify({"error": "Group not found"}), 404
    _config["groups"] = new_groups
    import yaml
    config_path = os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))
    backup_path = config_path + ".bak"
    if Path(config_path).exists():
        Path(backup_path).write_text(Path(config_path).read_text(encoding="utf-8"), encoding="utf-8")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(_config, f, allow_unicode=True, default_flow_style=False)
    return jsonify({"status": "removed", "groups": _config["groups"]})


# --- Crawl Control APIs ---

@api.route("/crawl/start", methods=["POST"])
@require_auth
def start_crawl():
    data = request.get_json(silent=True) or {}
    mode = data.get("mode", "incremental")
    if mode not in ("incremental", "full", "date_range"):
        return jsonify({"error": "Invalid mode. Use: incremental, full, date_range"}), 400
    ok = _crawl_service.start(mode=mode)
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
    import yaml
    config_path = os.environ.get("TGWATCHER_CONFIG", str(Path.cwd() / "config.yaml"))
    backup_path = config_path + ".bak"
    if Path(config_path).exists():
        Path(backup_path).write_text(Path(config_path).read_text(encoding="utf-8"), encoding="utf-8")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(_config, f, allow_unicode=True, default_flow_style=False)
    return jsonify({"status": "updated", "groups": _config["groups"]})


# --- Telegram Dialog API ---

@api.route("/dialogs", methods=["GET"])
@require_auth
def get_dialogs():
    tg = TGClient(_config)
    try:
        if _async_loop:
            _async_loop.run_coroutine(tg.connect())
            dialogs = _async_loop.run_coroutine(tg.list_dialogs())
            _async_loop.run_coroutine(tg.disconnect())
        else:
            import asyncio
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(tg.connect())
                dialogs = loop.run_until_complete(tg.list_dialogs())
                loop.run_until_complete(tg.disconnect())
            finally:
                loop.close()
        return jsonify(dialogs)
    except Exception as e:
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
                # Keep event list bounded
                with _sse_lock:
                    if len(_sse_events) > 200:
                        del _sse_events[:100]
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

def _safe_disconnect(loop, client):
    try:
        result = client.disconnect()
        if hasattr(result, '__await__'):
            loop.run_until_complete(result)
    except Exception:
        pass


@api.route("/login/status", methods=["GET"])
def login_status():
    ip = request.remote_addr or "unknown"
    if not _check_rate_limit(f"login_status:{ip}"):
        return jsonify({"error": "Rate limited"}), 429

    from telethon import TelegramClient

    phone = _config["telegram"]["phone"]
    session_dir = Path(_config["telegram"].get("session_dir", "./sessions"))
    safe_phone = phone.replace("+", "")
    session_file = session_dir / f"tgwatcher_{safe_phone}.session"

    connected = False
    if session_file.exists():
        try:
            proxy = None
            proxy_cfg = _config.get("proxy", {})
            if proxy_cfg.get("enabled", False):
                proxy = (proxy_cfg.get("protocol", "socks5"), proxy_cfg["host"], proxy_cfg["port"])
            client = TelegramClient(str(session_file), _config["telegram"]["api_id"], _config["telegram"]["api_hash"], proxy=proxy)
            if _async_loop:
                _async_loop.run_coroutine(client.connect())
                connected = _async_loop.run_coroutine(client.is_user_authorized())
                _async_loop.run_coroutine(client.disconnect())
            else:
                import asyncio
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(client.connect())
                    connected = loop.run_until_complete(client.is_user_authorized())
                    _safe_disconnect(loop, client)
                finally:
                    loop.close()
        except Exception as e:
            logger.warning("Login status check failed: %s", e)
            connected = False

    return jsonify({"logged_in": connected, "phone": phone})


@api.route("/login", methods=["POST"])
def do_login():
    ip = request.remote_addr or "unknown"
    if not _check_rate_limit(f"login:{ip}"):
        return jsonify({"error": "Rate limited"}), 429

    from telethon import TelegramClient

    data = request.get_json(silent=True) or {}
    code = data.get("code")
    phone_code_hash = data.get("phone_code_hash")

    phone = _config["telegram"]["phone"]
    session_dir = Path(_config["telegram"].get("session_dir", "./sessions"))
    safe_phone = phone.replace("+", "")
    session_path = str(session_dir / f"tgwatcher_{safe_phone}")

    proxy = None
    proxy_cfg = _config.get("proxy", {})
    if proxy_cfg.get("enabled", False):
        proxy = (proxy_cfg.get("protocol", "socks5"), proxy_cfg["host"], proxy_cfg["port"])

    use_shared = _async_loop is not None

    try:
        client = TelegramClient(session_path, _config["telegram"]["api_id"], _config["telegram"]["api_hash"], proxy=proxy)

        if use_shared:
            _async_loop.run_coroutine(client.connect())
            authorized = _async_loop.run_coroutine(client.is_user_authorized())
        else:
            import asyncio
            loop = asyncio.new_event_loop()
            loop.run_until_complete(client.connect())
            authorized = loop.run_until_complete(client.is_user_authorized())

        if authorized:
            if use_shared:
                _async_loop.run_coroutine(client.disconnect())
            else:
                _safe_disconnect(loop, client)
            return jsonify({"status": "already_logged_in"})

        if code and phone_code_hash:
            try:
                if use_shared:
                    _async_loop.run_coroutine(client.sign_in(phone, code, phone_code_hash=phone_code_hash))
                    _async_loop.run_coroutine(client.disconnect())
                else:
                    loop.run_until_complete(client.sign_in(phone, code, phone_code_hash=phone_code_hash))
                    _safe_disconnect(loop, client)
                return jsonify({"status": "logged_in"})
            except Exception as e:
                if use_shared:
                    _async_loop.run_coroutine(client.disconnect())
                else:
                    _safe_disconnect(loop, client)
                return jsonify({"error": str(e)}), 400
        else:
            try:
                if use_shared:
                    result = _async_loop.run_coroutine(client.send_code_request(phone))
                    _async_loop.run_coroutine(client.disconnect())
                else:
                    result = loop.run_until_complete(client.send_code_request(phone))
                    _safe_disconnect(loop, client)
                return jsonify({"status": "code_sent", "phone_code_hash": result.phone_code_hash})
            except Exception as e:
                if use_shared:
                    _async_loop.run_coroutine(client.disconnect())
                else:
                    _safe_disconnect(loop, client)
                return jsonify({"error": str(e)}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500

"""SSE streaming endpoint (Phase 2B batch 5).

Extracted from `._legacy`. Routes:
  GET /events — Server-Sent Events stream with Last-Event-ID replay.
"""
import json
import logging

from flask import Blueprint, Response, jsonify, request

from ._legacy import (
    _auth_token,
    _extract_auth_token,
    _sse_bus,
    require_auth,
)

logger = logging.getLogger(__name__)

bp = Blueprint("sse", __name__, url_prefix="")


@bp.route("/events", methods=["GET"])
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

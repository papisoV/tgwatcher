"""SSE event bus for TGWatcher.

Encapsulates the SSE pub/sub state that was previously held as 5 module-level
globals in `tgwatcher/web/api.py`. Extracted in Phase 2A (scoped down) of the
3-file refactor (plan: ticklish-cooking-glade.md).

Contract:
- `push(event_type, data)` — replaces `push_sse_event`
- `register_listener(last_id)` — returns `(listener_event, adjusted_last_id)`;
  handles fresh-connection and buffer-rollover cases
- `unregister_listener(listener_event)` — removes listener
- `events_since(last_id)` — returns events with id > last_id
- `trim_if_needed()` — keeps buffer <= MAX_SSE_EVENTS
- Properties: `current_event_id`, `oldest_event_id`, `listener_count`,
  `buffered_event_count` — for metrics/observability

The bus is thread-safe. All state is held inside the instance — no module
globals. api.py owns a singleton `_sse_bus = SSEBus()`.
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)

MAX_SSE_EVENTS = 200


class SSEBus:
    """Thread-safe pub/sub bus for SSE events."""

    def __init__(self, max_events: int = MAX_SSE_EVENTS) -> None:
        self._lock = threading.Lock()
        self._listeners: list[threading.Event] = []
        self._events: list[dict] = []
        self._event_id: int = 0
        self._max_events = max_events

    # --- Pub ---

    def push(self, event_type: str, data: dict) -> None:
        """Append event, notify all listeners. Thread-safe."""
        with self._lock:
            self._event_id += 1
            event = {"id": self._event_id, "type": event_type, "data": data}
            self._events.append(event)
            if len(self._events) > self._max_events:
                # Keep the newest MAX_SSE_EVENTS events. Older events are dropped;
                # clients reconnecting with last_id below the new floor are handled
                # by the Last-Event-ID fallback in register_listener.
                del self._events[: len(self._events) - self._max_events]
            for listener in self._listeners:
                listener.set()

    # --- Sub ---

    def register_listener(self, last_id: int) -> tuple[threading.Event, int]:
        """Register a new SSE listener.

        Returns (listener_event, adjusted_last_id). The caller must call
        unregister_listener when done. Handles three cases:
        - last_id == 0: fresh connection, start from current max (no replay)
        - last_id < oldest buffered: buffer rolled past; start from oldest-1
          to avoid stall (client will miss intermediate events)
        - otherwise: normal replay path
        """
        listener_event = threading.Event()
        with self._lock:
            self._listeners.append(listener_event)
            if last_id == 0:
                last_id = self._event_id
            elif self._events and last_id < self._events[0]["id"]:
                logger.warning(
                    "SSE reconnect: last_id=%d older than buffer floor=%d; "
                    "client will miss intermediate events. Use webhook + "
                    "/api/signals/export?since=<ts> for full compensation.",
                    last_id, self._events[0]["id"],
                )
                last_id = self._events[0]["id"] - 1
        return listener_event, last_id

    def unregister_listener(self, listener_event: threading.Event) -> None:
        with self._lock:
            if listener_event in self._listeners:
                self._listeners.remove(listener_event)

    def events_since(self, last_id: int) -> list[dict]:
        """Return events with id > last_id. Caller should update last_id to
        the id of the last event received."""
        with self._lock:
            return [e for e in self._events if e["id"] > last_id]

    def trim_if_needed(self) -> None:
        """Secondary buffer trim — called from the SSE generator loop."""
        with self._lock:
            if len(self._events) > self._max_events:
                del self._events[: len(self._events) - self._max_events]

    # --- Observability (for /api/metrics) ---

    @property
    def listener_count(self) -> int:
        return len(self._listeners)

    @property
    def buffered_event_count(self) -> int:
        return len(self._events)

    @property
    def current_event_id(self) -> int:
        return self._event_id

    @property
    def oldest_event_id(self) -> int | None:
        """Return the oldest buffered event's id, or None if buffer empty.
        Used for reconnect diagnostics."""
        with self._lock:
            return self._events[0]["id"] if self._events else None

    @property
    def max_events(self) -> int:
        return self._max_events

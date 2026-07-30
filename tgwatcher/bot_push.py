"""Telegram Bot push dispatcher for new signals.

Sends formatted signal alerts to subscribed Telegram chats via Bot API.
Mirrors WebhookDispatcher architecture: async dispatch via ThreadPoolExecutor,
failures logged but never block the caller, codename enforcement on all output.

Config (config.yaml):
  signal:
    bot:
      enabled: true
      token: "123456:ABC-DEF..."
      chat_ids:
        - 123456789
        - -1001234567890  # group/channel
      parse_mode: "HTML"  # HTML or Markdown
      timeout_seconds: 5
"""
from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

from tgwatcher.codename_map import codename_map

logger = logging.getLogger(__name__)

_TELEGRAM_API_BASE = "https://api.telegram.org"


class BotPusher:
    """Push new_signal payloads to Telegram chats via Bot API.

    Thread-safe. Failed sends are logged + counted but never block.
    All output uses codename enforcement (标的X codes, no real coin names).
    """

    def __init__(self, config: dict) -> None:
        self._token: str = ""
        self._chat_ids: list[int] = []
        self._parse_mode: str = "HTML"
        self._timeout: float = 5.0
        self._enabled: bool = False
        self._max_workers: int = 4
        self._executor: ThreadPoolExecutor | None = None
        self._lock = threading.Lock()
        self._success_count: int = 0
        self._fail_count: int = 0
        self._last_push_at: datetime | None = None

        try:
            bot_cfg = config.get("bot", {}) or {}
            self._enabled = bool(bot_cfg.get("enabled", False))
            self._token = str(bot_cfg.get("token", "")).strip()
            self._parse_mode = str(bot_cfg.get("parse_mode", "HTML")).strip()
            self._timeout = float(bot_cfg.get("timeout_seconds", 5))
            self._max_workers = int(bot_cfg.get("max_workers", 4))
            self._chat_ids = [int(cid) for cid in (bot_cfg.get("chat_ids") or []) if cid]

            if self._enabled and self._token and self._chat_ids:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._max_workers,
                    thread_name_prefix="bot-push",
                )
                logger.info(
                    "BotPusher initialized: %d chat(s), parse_mode=%s",
                    len(self._chat_ids), self._parse_mode,
                )
            else:
                logger.info(
                    "BotPusher disabled (enabled=%s, token=%s, chats=%d)",
                    self._enabled, bool(self._token), len(self._chat_ids),
                )
        except Exception as e:
            logger.error("BotPusher init failed: %s", e, exc_info=True)
            self._enabled = False

    @property
    def enabled(self) -> bool:
        return self._enabled and bool(self._token) and bool(self._chat_ids)

    def dispatch(self, signal_payload: dict) -> None:
        """Async-dispatch signal to all configured chat_ids. Returns immediately."""
        if not self.enabled or not self._executor:
            return

        text = self.format_signal(signal_payload)
        for chat_id in self._chat_ids:
            self._executor.submit(self._send, chat_id, text)

    def format_signal(self, payload: dict) -> str:
        """Format a new_signal payload into a codenamed Chinese alert message.

        All coin names are replaced with 标的X codes. Output is HTML-formatted
        for Telegram's parse_mode=HTML.
        """
        direction = payload.get("direction", 0)
        magnitude = payload.get("magnitude", 0)
        urgency = payload.get("urgency", 0)
        confidence = payload.get("confidence", 0)
        halflife_min = payload.get("halflife_min", 60)
        event_type = payload.get("event_type", "other")
        reasoning = payload.get("reasoning", "")
        signal_score = payload.get("signal_score", 0)
        chat_title = payload.get("chat_title", "")
        date_str = payload.get("date", "")

        # Codename enforcement on symbols and reasoning
        symbols_raw = payload.get("symbols", [])
        if isinstance(symbols_raw, str):
            try:
                symbols_raw = json.loads(symbols_raw)
            except (json.JSONDecodeError, TypeError):
                symbols_raw = []
        symbols = [codename_map.get_code(s) if s != "*" else "全市场" for s in symbols_raw]
        symbols_str = ", ".join(symbols) if symbols else "全市场"

        # Codename enforcement on reasoning
        safe_reasoning = codename_map.replace_names(str(reasoning))

        # Direction arrow
        if direction > 0.3:
            arrow = "🟢利多"
        elif direction < -0.3:
            arrow = "🔴利空"
        else:
            arrow = "⚪中性"

        # Urgency label
        if urgency >= 0.7:
            urgency_label = "⚡高"
        elif urgency >= 0.4:
            urgency_label = "🔶中"
        else:
            urgency_label = "🔹低"

        # Event type Chinese mapping
        event_type_map = {
            "security": "🛡️安全", "regulatory": "📜监管", "macro": "🌍宏观",
            "whale": "🐋鲸鱼", "market": "📊市场", "listing": "🆕上币",
            "partnership": "🤝合作", "other": "📌其他",
        }
        event_label = event_type_map.get(event_type, "📌其他")

        # Format time
        time_str = ""
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                time_str = dt.strftime("%H:%M")
            except (ValueError, TypeError):
                time_str = ""

        # Build message
        lines = [
            f"<b>{arrow} {symbols_str}</b>",
            f"{event_label} | 紧急度: {urgency_label} | 评分: {signal_score:.2f}",
        ]
        if chat_title:
            lines.append(f"来源: {chat_title}")
        if time_str:
            lines.append(f"时间: {time_str}")
        lines.append(f"推理: {safe_reasoning}")

        return "\n".join(lines)

    def _send(self, chat_id: int, text: str) -> None:
        """Send one message via Bot API. Records success/failure."""
        url = f"{_TELEGRAM_API_BASE}/bot{self._token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": self._parse_mode,
        }
        try:
            r = requests.post(url, json=payload, timeout=self._timeout)
            if 200 <= r.status_code < 300:
                with self._lock:
                    self._success_count += 1
                    self._last_push_at = datetime.now(timezone.utc)
                logger.info("Bot push to chat %d: OK", chat_id)
            else:
                self._record_failure(chat_id, f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            self._record_failure(chat_id, f"{type(e).__name__}: {e}")

    def _record_failure(self, chat_id: int, err: str) -> None:
        with self._lock:
            self._fail_count += 1
        logger.warning("Bot push to chat %d failed: %s", chat_id, err)

    def get_status(self) -> dict:
        """Return status snapshot for GET /api/bot/status."""
        with self._lock:
            return {
                "enabled": self.enabled,
                "chat_ids": list(self._chat_ids),
                "last_push_at": (
                    self._last_push_at.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                    if self._last_push_at else None
                ),
                "success_count": self._success_count,
                "fail_count": self._fail_count,
            }

    def update_config(self, enabled: bool | None = None, chat_ids: list[int] | None = None) -> None:
        """Update config at runtime without restart."""
        with self._lock:
            if enabled is not None:
                self._enabled = enabled
            if chat_ids is not None:
                self._chat_ids = [int(cid) for cid in chat_ids]
            # Ensure executor exists if now enabled
            if self._enabled and self._token and self._chat_ids and not self._executor:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._max_workers,
                    thread_name_prefix="bot-push",
                )
        logger.info("BotPusher config updated: enabled=%s, chats=%d", self._enabled, len(self._chat_ids))

    def send_test(self) -> dict:
        """Send a test signal to all configured chat_ids."""
        test_payload = {
            "message_id": 0,
            "chat_id": 0,
            "chat_title": "[TEST]",
            "text": "Bot push test from TGWatcher",
            "direction": 0.8,
            "magnitude": 0.7,
            "urgency": 0.6,
            "confidence": 0.9,
            "halflife_min": 60,
            "symbols": ["BTC", "ETH"],
            "event_type": "market",
            "reasoning": "测试推送 — BTC和ETH出现市场异动",
            "signal_score": 0.8 * 0.7 * 0.9 * (0.5 + 0.5 * 0.6),
            "date": datetime.now(timezone.utc).isoformat(),
        }
        text = self.format_signal(test_payload)
        results: list[dict] = []

        if not self.enabled:
            return {"status": "disabled", "results": []}

        for chat_id in self._chat_ids:
            url = f"{_TELEGRAM_API_BASE}/bot{self._token}/sendMessage"
            payload = {"chat_id": chat_id, "text": text, "parse_mode": self._parse_mode}
            try:
                r = requests.post(url, json=payload, timeout=self._timeout)
                results.append({
                    "chat_id": chat_id,
                    "ok": 200 <= r.status_code < 300,
                    "status_code": r.status_code,
                })
            except Exception as e:
                results.append({
                    "chat_id": chat_id,
                    "ok": False,
                    "error": f"{type(e).__name__}: {e}",
                })
        return {"status": "sent", "results": results}

"""Real-time message listener (reserved for future use).

Enable with: python main.py --listen
"""
import asyncio
import logging
from datetime import datetime

from telethon import events

from tgwatcher.storage import Storage

logger = logging.getLogger(__name__)


async def start_listener(client: "TGClient", storage: Storage, groups: list[dict], on_new_message=None) -> None:
    """Listen for new messages in real-time and save to DB.

    Args:
        on_new_message: Optional callback invoked with msg dict after saving.
                        Used to push real-time updates via SSE.
    """
    tg = client.client
    if tg is None:
        raise RuntimeError("Client not connected")

    chat_ids = set()
    for g in groups:
        chat_id = g.get("id") or g.get("username")
        if chat_id:
            chat_ids.add(chat_id)

    @tg.on(events.NewMessage)
    async def handler(event):
        chat_id = event.chat_id
        if chat_id not in chat_ids:
            return

        text = event.raw_text
        if not text:
            return

        forward_from = None
        if event.message.forward:
            if event.message.forward.sender:
                forward_from = getattr(event.message.forward.sender, "first_name", None)
            elif event.message.forward.chat:
                forward_from = getattr(event.message.forward.chat, "title", None)

        sender_name = None
        sender_username = None
        if event.sender:
            sender_name = getattr(event.sender, "first_name", None)
            last = getattr(event.sender, "last_name", None)
            if last:
                sender_name = f"{sender_name} {last}"
            sender_username = getattr(event.sender, "username", None)

        msg = {
            "message_id": event.message.id,
            "chat_id": chat_id,
            "chat_title": getattr(event.chat, "title", None),
            "sender_id": event.sender_id,
            "sender_name": sender_name,
            "sender_username": sender_username,
            "text": text,
            "reply_to_msg_id": event.message.reply_to_msg_id if event.message.is_reply else None,
            "forward_from": forward_from,
            "date": event.message.date,
            "has_media": event.message.media is not None,
        }
        storage.save_messages([msg])
        if on_new_message:
            try:
                on_new_message(msg)
            except Exception:
                pass
        logger.info("[LIVE] %s in %s: %s", sender_name or "Unknown", msg.get("chat_title"), text[:50])

    logger.info("Real-time listener started for %d chats", len(chat_ids))
    await tg.run_until_disconnected()

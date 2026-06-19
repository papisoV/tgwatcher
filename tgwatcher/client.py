import asyncio
import logging
import random
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import Channel, Chat

logger = logging.getLogger(__name__)


class TGClient:
    def __init__(self, config: dict):
        tg = config["telegram"]
        proxy_cfg = config["proxy"]

        self.api_id = tg["api_id"]
        self.api_hash = tg["api_hash"]
        self.phone = tg["phone"]
        self.session_dir = Path(tg.get("session_dir", "./sessions"))
        self.session_dir.mkdir(parents=True, exist_ok=True)

        self.proxy = None
        if proxy_cfg.get("enabled", False):
            self.proxy = (
                proxy_cfg.get("protocol", "socks5"),
                proxy_cfg["host"],
                proxy_cfg["port"],
            )

        crawl = config.get("crawl", {})
        self.min_delay = crawl.get("min_delay", 1)
        self.max_delay = crawl.get("max_delay", 3)

        self.client: TelegramClient | None = None

    def _session_path(self) -> str:
        safe_phone = self.phone.replace("+", "")
        return str(self.session_dir / f"tgwatcher_{safe_phone}")

    async def connect(self) -> None:
        self.client = TelegramClient(
            self._session_path(),
            self.api_id,
            self.api_hash,
            proxy=self.proxy,
        )
        await self.client.start(phone=self.phone)
        me = await self.client.get_me()
        logger.info("Logged in as %s (ID: %s)", me.first_name, me.id)

    async def disconnect(self) -> None:
        if self.client and self.client.is_connected():
            await self.client.disconnect()

    async def _get_client(self) -> TelegramClient:
        if self.client is None:
            self.client = TelegramClient(
                self._session_path(),
                self.api_id,
                self.api_hash,
                proxy=self.proxy,
            )
        return self.client

    async def login_interactive(self) -> None:
        self.client = TelegramClient(
            self._session_path(),
            self.api_id,
            self.api_hash,
            proxy=self.proxy,
        )
        await self.client.start(phone=self.phone)
        me = await self.client.get_me()
        logger.info("Login successful! User: %s (ID: %s)", me.first_name, me.id)
        await self.client.disconnect()

    async def list_dialogs(self) -> list[dict]:
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")
        dialogs = []
        async for dialog in self.client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (Channel, Chat)):
                dialogs.append({
                    "id": entity.id,
                    "title": dialog.name,
                    "username": getattr(entity, "username", None),
                    "is_channel": isinstance(entity, Channel) and not entity.megagroup,
                    "is_group": isinstance(entity, Chat) or (isinstance(entity, Channel) and entity.megagroup),
                    "members": getattr(entity, "participants_count", None),
                })
        return dialogs

    async def fetch_messages(
        self,
        chat_id: int | str,
        limit: int = 100,
        min_id: int = 0,
        offset_date: datetime | None = None,
        until_date: datetime | None = None,
    ) -> list[dict]:
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")

        entity = await self.client.get_entity(chat_id)
        chat_title = getattr(entity, "title", str(chat_id))

        messages = []
        async for msg in self.client.iter_messages(
            entity, limit=limit, min_id=min_id, offset_date=offset_date, reverse=True
        ):
            if msg.text is None:
                continue

            # 按时间范围过滤
            if until_date and msg.date and msg.date > until_date:
                continue

            forward_from = None
            if msg.forward:
                if msg.forward.sender:
                    forward_from = getattr(msg.forward.sender, "first_name", None) or getattr(msg.forward.sender, "title", None)
                elif msg.forward.chat:
                    forward_from = getattr(msg.forward.chat, "title", None)

            sender_name = None
            sender_username = None
            if msg.sender:
                sender_name = getattr(msg.sender, "first_name", None) or getattr(msg.sender, "title", None)
                last = getattr(msg.sender, "last_name", None)
                if last:
                    sender_name = f"{sender_name} {last}"
                sender_username = getattr(msg.sender, "username", None)

            messages.append({
                "message_id": msg.id,
                "chat_id": entity.id,
                "chat_title": chat_title,
                "sender_id": msg.sender_id,
                "sender_name": sender_name,
                "sender_username": sender_username,
                "text": msg.text,
                "reply_to_msg_id": msg.reply_to_msg_id if msg.is_reply else None,
                "forward_from": forward_from,
                "date": msg.date,
                "has_media": msg.media is not None,
            })

            delay = random.uniform(self.min_delay, self.max_delay)
            await asyncio.sleep(delay)

        logger.info("Fetched %d messages from %s", len(messages), chat_title)
        return messages

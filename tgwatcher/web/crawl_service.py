"""Crawl service - runs Telethon crawl in a background thread, controllable via start/stop/status."""
import asyncio
import logging
import threading
from datetime import datetime
from typing import Callable

from tgwatcher.client import TGClient
from tgwatcher.storage import Storage

logger = logging.getLogger(__name__)


class CrawlService:
    def __init__(self, config: dict, async_loop=None, on_status_change: Callable | None = None):
        self.config = config
        self._async_loop = async_loop
        self._on_status_change = on_status_change
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._status = {
            "running": False,
            "mode": "idle",
            "current_group": None,
            "total_groups": 0,
            "completed_groups": 0,
            "fetched": 0,
            "saved": 0,
            "started_at": None,
            "last_crawl_at": None,
            "error": None,
        }

    @property
    def status(self) -> dict:
        with self._lock:
            return dict(self._status)

    def _update_status(self, **kwargs) -> None:
        with self._lock:
            self._status.update(kwargs)
        if self._on_status_change:
            try:
                self._on_status_change(self.status)
            except Exception:
                pass

    def start(self, mode: str = "incremental") -> bool:
        with self._lock:
            if self._status["running"]:
                return False
        self._stop_event.clear()
        self._update_status(
            running=True,
            mode=mode,
            current_group=None,
            total_groups=len(self.config.get("groups", [])),
            completed_groups=0,
            fetched=0,
            saved=0,
            started_at=datetime.now().isoformat(),
            last_crawl_at=None,
            error=None,
        )
        thread = threading.Thread(target=self._run_loop, args=(mode,), daemon=True)
        thread.start()
        logger.info("Crawl service started (mode=%s)", mode)
        return True

    def stop(self) -> bool:
        with self._lock:
            if not self._status["running"]:
                return False
        self._stop_event.set()
        logger.info("Crawl service stop requested")
        return True

    def _run_loop(self, mode: str) -> None:
        try:
            if self._async_loop:
                loop = self._async_loop.get_loop()
                future = asyncio.run_coroutine_threadsafe(self._crawl(loop, mode), loop)
                future.result()
            else:
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(self._crawl(loop, mode))
                finally:
                    loop.close()
        except Exception as e:
            logger.error("Crawl service error: %s", e)
            self._update_status(error=str(e))
        finally:
            self._update_status(running=False, current_group=None)

    async def _crawl(self, loop: asyncio.AbstractEventLoop, mode: str) -> None:
        groups = self.config.get("groups", [])
        if not groups:
            self._update_status(error="No groups configured")
            return

        crawl_cfg = self.config.get("crawl", {})
        limit = crawl_cfg.get("limit", 100)
        interval = crawl_cfg.get("interval_minutes", 30)

        offset_date = None
        until_date = None
        if crawl_cfg.get("offset_date"):
            offset_date = datetime.fromisoformat(crawl_cfg["offset_date"])
        if crawl_cfg.get("until_date"):
            until_date = datetime.fromisoformat(crawl_cfg["until_date"])

        db_path = self.config["storage"]["db_path"]
        storage = Storage(db_path)
        storage.init_db()

        tg = TGClient(self.config)
        await tg.connect()

        try:
            iteration = 0
            while not self._stop_event.is_set():
                iteration += 1
                completed = 0
                for group in groups:
                    if self._stop_event.is_set():
                        break

                    chat_id = group.get("id") or group.get("username")
                    group_name = group.get("name", chat_id)
                    if not chat_id:
                        continue

                    self._update_status(current_group=group_name, completed_groups=completed)

                    if mode == "full":
                        min_id = 0
                    elif mode == "date_range":
                        min_id = 0
                    else:
                        last_id = storage.get_last_message_id(
                            chat_id if isinstance(chat_id, int) else 0
                        )
                        min_id = last_id if last_id else 0

                    try:
                        messages = await tg.fetch_messages(
                            chat_id=chat_id,
                            limit=limit,
                            min_id=min_id,
                            offset_date=offset_date if mode == "date_range" else None,
                            until_date=until_date if mode == "date_range" else None,
                        )
                        with self._lock:
                            self._status["fetched"] += len(messages)
                        if messages:
                            saved = storage.save_messages(messages)
                            with self._lock:
                                self._status["saved"] += saved
                            logger.info("Saved %d/%d messages from %s", saved, len(messages), group_name)
                        else:
                            logger.info("No new messages from %s", group_name)
                    except Exception as e:
                        logger.error("Error crawling %s: %s", group_name, e)

                    completed += 1

                self._update_status(
                    last_crawl_at=datetime.now().isoformat(),
                    completed_groups=completed,
                )

                if mode != "incremental":
                    break

                if self._stop_event.is_set():
                    break

                for _ in range(interval * 12):
                    if self._stop_event.is_set():
                        break
                    await asyncio.sleep(5)
        finally:
            await tg.disconnect()

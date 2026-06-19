import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from tgwatcher.models import Base, Message

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        self._session_factory = sessionmaker(bind=self.engine)

    def init_db(self) -> None:
        Base.metadata.create_all(self.engine)
        with self.engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.commit()
        logger.info("Database initialized (WAL mode)")

    def get_session(self) -> Session:
        return self._session_factory()

    @staticmethod
    def _ensure_datetime(value):
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None

    def save_messages(self, messages: list[dict]) -> int:
        if not messages:
            return 0
        saved = 0
        with self.get_session() as session:
            for msg in messages:
                record = Message(
                    message_id=msg["message_id"],
                    chat_id=msg["chat_id"],
                    chat_title=msg.get("chat_title"),
                    sender_id=msg.get("sender_id"),
                    sender_name=msg.get("sender_name"),
                    sender_username=msg.get("sender_username"),
                    text=msg.get("text"),
                    reply_to_msg_id=msg.get("reply_to_msg_id"),
                    forward_from=msg.get("forward_from"),
                    date=self._ensure_datetime(msg.get("date")),
                    has_media=msg.get("has_media", False),
                    crawled_at=datetime.now(),
                )
                session.merge(record)
                saved += 1
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                logger.warning("Integrity error on batch save, skipping duplicates")
        return saved

    def get_last_message_id(self, chat_id: int) -> int | None:
        with self.get_session() as session:
            result = (
                session.query(func.max(Message.message_id))
                .filter(Message.chat_id == chat_id)
                .scalar()
            )
        return result

    def get_stats(self) -> dict:
        with self.get_session() as session:
            total = session.query(func.count(Message.id)).scalar()
            chat_count = session.query(func.count(func.distinct(Message.chat_id))).scalar()
            earliest = session.query(func.min(Message.date)).scalar()
            latest = session.query(func.max(Message.date)).scalar()
        return {
            "total_messages": total,
            "monitored_chats": chat_count,
            "earliest_message": earliest,
            "latest_message": latest,
        }

    def query_messages(
        self,
        chat_id: int | None = None,
        keyword: str | None = None,
        sender_id: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict:
        with self.get_session() as session:
            q = session.query(Message)
            if chat_id is not None:
                q = q.filter(Message.chat_id == chat_id)
            if keyword:
                escaped = keyword.replace("%", "\\%").replace("_", "\\_")
                q = q.filter(Message.text.ilike(f"%{escaped}%", escape="\\"))
            if sender_id is not None:
                q = q.filter(Message.sender_id == sender_id)
            if date_from:
                q = q.filter(Message.date >= date_from)
            if date_to:
                q = q.filter(Message.date <= date_to)

            total = q.count()
            rows = (
                q.order_by(Message.date.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
                .all()
            )
            messages = []
            for r in rows:
                messages.append({
                    "id": r.id,
                    "message_id": r.message_id,
                    "chat_id": r.chat_id,
                    "chat_title": r.chat_title,
                    "sender_id": r.sender_id,
                    "sender_name": r.sender_name,
                    "sender_username": r.sender_username,
                    "text": r.text,
                    "reply_to_msg_id": r.reply_to_msg_id,
                    "forward_from": r.forward_from,
                    "date": r.date.isoformat() if r.date else None,
                    "has_media": r.has_media,
                })
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "messages": messages,
        }

    def get_chats(self) -> list[dict]:
        with self.get_session() as session:
            rows = (
                session.query(
                    Message.chat_id,
                    Message.chat_title,
                    func.count(Message.id).label("msg_count"),
                    func.max(Message.date).label("last_msg_date"),
                )
                .group_by(Message.chat_id, Message.chat_title)
                .all()
            )
        return [
            {
                "chat_id": r.chat_id,
                "chat_title": r.chat_title,
                "msg_count": r.msg_count,
                "last_msg_date": r.last_msg_date.isoformat() if r.last_msg_date else None,
            }
            for r in rows
        ]

    def get_message_trend(self, period: str = "day", days: int = 30, chat_id: int | None = None) -> dict:
        from sqlalchemy import cast, Date, extract
        with self.get_session() as session:
            q = session.query(
                Message.chat_id,
                Message.chat_title,
                cast(Message.date, Date).label("msg_date"),
                func.count(Message.id).label("count"),
            )
            if chat_id is not None:
                q = q.filter(Message.chat_id == chat_id)
            q = q.filter(Message.date >= func.date("now", f"-{days} days"))
            q = q.group_by(Message.chat_id, Message.chat_title, cast(Message.date, Date))
            q = q.order_by(cast(Message.date, Date))
            rows = q.all()

        datasets: dict[tuple, dict] = {}
        all_dates: set = set()
        for r in rows:
            key = (r.chat_id, r.chat_title or "Unknown")
            if key not in datasets:
                datasets[key] = {"chat_id": r.chat_id, "chat_title": r.chat_title or "Unknown", "data": {}}
            date_str = r.msg_date.isoformat() if r.msg_date else ""
            datasets[key]["data"][date_str] = r.count
            all_dates.add(date_str)

        sorted_dates = sorted(all_dates)
        result = {
            "labels": sorted_dates,
            "datasets": [
                {
                    "chat_id": ds["chat_id"],
                    "chat_title": ds["chat_title"],
                    "data": [ds["data"].get(d, 0) for d in sorted_dates],
                }
                for ds in datasets.values()
            ],
        }
        return result

    def get_activity_heatmap(self, chat_id: int | None = None) -> dict:
        with self.get_session() as session:
            from sqlalchemy import extract
            q = session.query(
                extract("hour", Message.date).label("hour"),
                extract("dow", Message.date).label("dow"),
                func.count(Message.id).label("count"),
            )
            if chat_id is not None:
                q = q.filter(Message.chat_id == chat_id)
            q = q.group_by(extract("hour", Message.date), extract("dow", Message.date))
            rows = q.all()

        data = []
        for r in rows:
            hour = int(r.hour) if r.hour is not None else 0
            dow = int(r.dow) if r.dow is not None else 0
            data.append({"hour": hour, "dow": dow, "count": r.count})
        return {"data": data}

    def get_group_comparison(self) -> dict:
        from sqlalchemy import func, distinct
        with self.get_session() as session:
            rows = (
                session.query(
                    Message.chat_id,
                    Message.chat_title,
                    func.count(Message.id).label("msg_count"),
                    func.count(distinct(Message.sender_id)).label("active_senders"),
                    func.min(Message.date).label("earliest"),
                    func.max(Message.date).label("latest"),
                )
                .group_by(Message.chat_id, Message.chat_title)
                .order_by(func.count(Message.id).desc())
                .limit(20)
                .all()
            )
        groups = []
        for r in rows:
            days_span = 1
            if r.earliest and r.latest:
                days_span = max(1, (r.latest - r.earliest).days)
            groups.append({
                "chat_id": r.chat_id,
                "chat_title": r.chat_title,
                "msg_count": r.msg_count,
                "active_senders": r.active_senders,
                "avg_per_day": round(r.msg_count / days_span, 1),
            })
        return {"groups": groups}

    def get_senders(self, chat_id: int | None = None) -> list[dict]:
        with self.get_session() as session:
            q = session.query(
                Message.sender_id,
                Message.sender_name,
                func.count(Message.id).label("msg_count"),
            ).filter(Message.sender_id.isnot(None))
            if chat_id is not None:
                q = q.filter(Message.chat_id == chat_id)
            q = q.group_by(Message.sender_id, Message.sender_name)
            q = q.order_by(func.count(Message.id).desc())
            rows = q.all()
        return [{"sender_id": r.sender_id, "sender_name": r.sender_name, "msg_count": r.msg_count} for r in rows]

    def get_message_by_id(self, message_id: int) -> dict | None:
        with self.get_session() as session:
            msg = session.query(Message).filter(Message.message_id == message_id).first()
            if not msg:
                return None
        return {
            "message_id": msg.message_id,
            "chat_id": msg.chat_id,
            "chat_title": msg.chat_title,
            "sender_id": msg.sender_id,
            "sender_name": msg.sender_name,
            "sender_username": msg.sender_username,
            "text": msg.text,
            "reply_to_msg_id": msg.reply_to_msg_id,
            "forward_from": msg.forward_from,
            "date": msg.date.isoformat() if msg.date else None,
            "has_media": msg.has_media,
        }

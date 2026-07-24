"""Chat repository — encapsulates chat-related persistence logic.

Extracted from the monolithic `Storage` class during Phase 1A refactor.
The repository owns no engine; it borrows the Storage facade's session
factory via composition.

Contract: signatures match the original Storage methods byte-for-byte,
so callers (`api.py`, `signal_engine.py`, etc.) need no changes.
"""
from __future__ import annotations

import logging
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from tgwatcher.models import Chat, Message
from tgwatcher.schemas import ParsedChat
from tgwatcher.tz_utils import utc_now

logger = logging.getLogger(__name__)


class ChatRepository:
    """Persistence operations for the `chats` table."""

    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory

    def upsert_chat_from_parsed(self, chat: ParsedChat) -> None:
        with self._session_factory() as session:
            row = session.query(Chat).filter(Chat.chat_id == chat.chat_id).first()
            if row:
                if chat.chat_title is not None:
                    row.chat_title = chat.chat_title
                if chat.chat_username is not None:
                    row.chat_username = chat.chat_username
                if chat.chat_type is not None:
                    row.chat_type = chat.chat_type
                if chat.members is not None:
                    row.members = chat.members
                row.updated_at = utc_now()
            else:
                row = Chat(
                    chat_id=chat.chat_id,
                    chat_title=chat.chat_title,
                    chat_username=chat.chat_username,
                    chat_type=chat.chat_type,
                    members=chat.members,
                )
                session.add(row)
            session.commit()

    def get_chats(self) -> list[dict]:
        with self._session_factory() as session:
            chat_rows = session.query(Chat).all()
            if chat_rows:
                result = []
                for chat in chat_rows:
                    msg_count = session.query(func.count(Message.id)).filter(
                        Message.chat_id == chat.chat_id, Message.is_deleted == False
                    ).scalar()
                    last_date = session.query(func.max(Message.date)).filter(
                        Message.chat_id == chat.chat_id
                    ).scalar()
                    result.append({
                        "chat_id": chat.chat_id,
                        "chat_title": chat.chat_title,
                        "chat_username": chat.chat_username,
                        "chat_type": chat.chat_type,
                        "members": chat.members,
                        "msg_count": msg_count,
                        "last_msg_date": last_date.isoformat() if last_date else None,
                    })
                return result

            # Fallback: derive from messages (pre-migration)
            rows = (
                session.query(
                    Message.chat_id,
                    Message.chat_title,
                    func.count(Message.id).label("msg_count"),
                    func.max(Message.date).label("last_msg_date"),
                )
                .filter(Message.is_deleted == False)
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

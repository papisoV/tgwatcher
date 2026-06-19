from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("message_id", "chat_id", name="uq_message_chat"),
        Index("ix_messages_date", "date"),
        Index("ix_messages_chat_id_date", "chat_id", "date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    chat_title: Mapped[str | None] = mapped_column(String(256))
    sender_id: Mapped[int | None] = mapped_column(BigInteger)
    sender_name: Mapped[str | None] = mapped_column(String(256))
    sender_username: Mapped[str | None] = mapped_column(String(128))
    text: Mapped[str | None] = mapped_column(String(4096))
    reply_to_msg_id: Mapped[int | None] = mapped_column(BigInteger)
    forward_from: Mapped[str | None] = mapped_column(String(256))
    date: Mapped[datetime | None] = mapped_column(DateTime)
    has_media: Mapped[bool] = mapped_column(Boolean, default=False)
    crawled_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

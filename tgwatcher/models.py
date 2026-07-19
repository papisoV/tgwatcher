from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Index, Integer, String, Text, UniqueConstraint, text as sa_text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SchemaVersion(Base):
    __tablename__ = "schema_version"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)


class Chat(Base):
    __tablename__ = "chats"

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    chat_title: Mapped[str | None] = mapped_column(String(256))
    chat_username: Mapped[str | None] = mapped_column(String(128))
    chat_type: Mapped[str | None] = mapped_column(String(20))
    members: Mapped[int | None] = mapped_column(Integer)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), server_default=sa_text("(datetime('now'))"))


class Sender(Base):
    __tablename__ = "senders"

    sender_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    sender_name: Mapped[str | None] = mapped_column(String(256))
    sender_username: Mapped[str | None] = mapped_column(String(128))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), server_default=sa_text("(datetime('now'))"))


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("message_id", "chat_id", name="uq_message_chat"),
        # removed explicit Index — covered by ix_messages_chat_id_date
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    chat_title: Mapped[str | None] = mapped_column(String(256))
    sender_id: Mapped[int | None] = mapped_column(BigInteger)
    sender_name: Mapped[str | None] = mapped_column(String(256))
    sender_username: Mapped[str | None] = mapped_column(String(128))
    text: Mapped[str | None] = mapped_column(Text)
    reply_to_msg_id: Mapped[int | None] = mapped_column(BigInteger)
    forward_from: Mapped[str | None] = mapped_column(String(256))
    date: Mapped[datetime | None] = mapped_column(DateTime, index=True)
    has_media: Mapped[bool] = mapped_column(Boolean, default=False)
    is_edited: Mapped[bool] = mapped_column(Boolean, default=False)
    edited_at: Mapped[datetime | None] = mapped_column(DateTime)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    media_type: Mapped[str | None] = mapped_column(String(32))
    media_id: Mapped[str | None] = mapped_column(String(256))
    crawled_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), server_default=sa_text("(datetime('now'))"))


class SignalFactor(Base):
    __tablename__ = "signal_factors"
    __table_args__ = (
        UniqueConstraint("message_id", "chat_id", name="uq_factor_message_chat"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_signal: Mapped[bool] = mapped_column(Boolean, default=True)  # True=signal, False=noise
    # Factor dimensions (float for quantitative weighting)
    direction: Mapped[float | None] = mapped_column(Float)          # [-1.0, 1.0] negative=bearish, positive=bullish
    magnitude: Mapped[float | None] = mapped_column(Float)          # [0.0, 1.0] impact strength
    urgency: Mapped[float | None] = mapped_column(Float)            # [0.0, 1.0] time-sensitivity
    confidence: Mapped[float | None] = mapped_column(Float)         # [0.0, 1.0] LLM judgment confidence
    halflife_min: Mapped[int | None] = mapped_column(Integer)       # >= 1, decay half-life in minutes
    symbols: Mapped[str | None] = mapped_column(Text)               # JSON array: '["BTC","ETH"]' or '["*"]'
    event_type: Mapped[str | None] = mapped_column(String(32))      # security|regulatory|macro|whale|market|listing|partnership|other
    reasoning: Mapped[str | None] = mapped_column(Text)             # LLM reasoning, <= 200 chars
    # Processing metadata
    llm_status: Mapped[str] = mapped_column(String(20), default="pending")  # pending/processing/completed/failed/skipped
    llm_error: Mapped[str | None] = mapped_column(String(256))
    llm_model: Mapped[str | None] = mapped_column(String(64))
    llm_raw: Mapped[str | None] = mapped_column(Text)                       # raw LLM response for debugging
    factor_version: Mapped[int] = mapped_column(Integer, default=2)         # v2 = new schema
    # Keyword filter metadata
    filter_result: Mapped[str | None] = mapped_column(String(16))           # passed/rejected
    matched_keywords: Mapped[str | None] = mapped_column(Text)              # JSON array
    keyword_preliminary: Mapped[str | None] = mapped_column(Text)           # JSON dict
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), server_default=sa_text("(datetime('now'))"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), server_default=sa_text("(datetime('now'))"))


# Composite index for chat+date queries
Index("ix_messages_chat_id_date", Message.chat_id, Message.date)

# SignalFactor indexes
Index("ix_signal_factors_message_id", SignalFactor.message_id)
Index("ix_signal_factors_chat_id", SignalFactor.chat_id)
Index("ix_signal_factors_llm_status", SignalFactor.llm_status)
Index("ix_signal_factors_event_type", SignalFactor.event_type)

"""SQLAlchemy 2.0 async-compatible ORM models for Open Claw."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> uuid.UUID:
    return uuid.uuid4()


class Base(DeclarativeBase):
    """Shared declarative base for all Open Claw models."""


# ── Tier 1: Raw ingestion ─────────────────────────────────────────────

class RawPost(Base):
    __tablename__ = "raw_posts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    platform: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(512), unique=True, nullable=False)
    author: Mapped[str] = mapped_column(String(256), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    tokens_mentioned: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    raw_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    processed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    filtered_posts: Mapped[list[FilteredPost]] = relationship(back_populates="raw_post")

    __table_args__ = (
        Index("ix_raw_posts_platform_scraped_processed", "platform", "scraped_at", "processed"),
    )


# ── Tier 2: Gatekeeper output ─────────────────────────────────────────

class FilteredPost(Base):
    __tablename__ = "filtered_posts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    raw_post_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("raw_posts.id"), nullable=False)
    relevance_score: Mapped[float] = mapped_column(Float, nullable=False)
    urgency: Mapped[str] = mapped_column(String(16), nullable=False)  # HIGH / MEDIUM / LOW
    category: Mapped[str] = mapped_column(String(128), nullable=False)
    gatekeeper_reasoning: Mapped[str] = mapped_column(Text, nullable=False)
    filtered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    analyzed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    raw_post: Mapped[RawPost] = relationship(back_populates="filtered_posts")
    sentiment_scores: Mapped[list[SentimentScore]] = relationship(back_populates="filtered_post")

    __table_args__ = (
        Index("ix_filtered_posts_urgency_category_analyzed", "urgency", "category", "analyzed"),
    )


# ── Entities ───────────────────────────────────────────────────────────

class Entity(Base):
    __tablename__ = "entities"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)  # COMPANY / COMMODITY / COUNTRY / SECTOR / PERSON / PRODUCT / LEGISLATION / CRYPTO
    ticker: Mapped[str | None] = mapped_column(String(32), nullable=True)
    related_tickers: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    sentiment_scores: Mapped[list[SentimentScore]] = relationship(back_populates="entity")

    __table_args__ = (
        UniqueConstraint("name", "entity_type", name="uq_entity_name_type"),
        Index("ix_entities_ticker_type", "ticker", "entity_type"),
    )


# ── Tier 3: Brain output ──────────────────────────────────────────────

class SentimentScore(Base):
    __tablename__ = "sentiment_scores"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    filtered_post_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("filtered_posts.id"), nullable=False)
    entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("entities.id"), nullable=False)
    sentiment: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    urgency: Mapped[float] = mapped_column(Float, nullable=False)
    reasoning: Mapped[str] = mapped_column(Text, nullable=False)
    themes: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    mechanism: Mapped[str | None] = mapped_column(Text, nullable=True)
    scored_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    filtered_post: Mapped[FilteredPost] = relationship(back_populates="sentiment_scores")
    entity: Mapped[Entity] = relationship(back_populates="sentiment_scores")


# ── Signals ────────────────────────────────────────────────────────────

class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    signal_type: Mapped[str] = mapped_column(String(64), nullable=False)
    affected_instruments: Mapped[dict] = mapped_column(JSONB, nullable=False)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)  # LONG / SHORT / HEDGE
    conviction: Mapped[float] = mapped_column(Float, nullable=False)
    time_horizon: Mapped[str] = mapped_column(String(64), nullable=False)
    thesis: Mapped[str] = mapped_column(Text, nullable=False)
    source_posts: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_signals_type_created_direction", "signal_type", "created_at", "direction"),
    )


# ── Market data cache ─────────────────────────────────────────────────

class PriceData(Base):
    __tablename__ = "price_data"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_1h: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_24h: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_price_data_symbol_fetched", "symbol", "fetched_at"),
    )

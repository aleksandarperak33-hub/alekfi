"""Shared utilities: logging, retry decorator, rate limiter, time helpers."""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

T = TypeVar("T")


# ── Structured JSON logging ───────────────────────────────────────────

class _JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> None:
    """Configure root logger with structured JSON output to stderr."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JSONFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


# ── Retry decorator with exponential backoff ──────────────────────────

def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable:
    """Async retry decorator with exponential backoff.

    Usage::

        @retry(max_attempts=5, base_delay=0.5)
        async def flaky_call():
            ...
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await fn(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        wait = base_delay * (2 ** (attempt - 1))
                        logging.getLogger(fn.__module__).warning(
                            "%s failed (attempt %d/%d): %s — retrying in %.1fs",
                            fn.__qualname__,
                            attempt,
                            max_attempts,
                            exc,
                            wait,
                        )
                        await asyncio.sleep(wait)
            raise RuntimeError(
                f"{fn.__qualname__} failed after {max_attempts} attempts"
            ) from last_exc

        return wrapper

    return decorator


# ── Rate limiter ──────────────────────────────────────────────────────

class RateLimiter:
    """Simple token-bucket rate limiter for async code.

    Usage::

        limiter = RateLimiter(max_calls=30, period=60)
        async with limiter:
            await do_api_call()
    """

    def __init__(self, max_calls: int, period: float = 60.0) -> None:
        self._max_calls = max_calls
        self._period = period
        self._calls: list[float] = []
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> RateLimiter:
        async with self._lock:
            now = time.monotonic()
            self._calls = [t for t in self._calls if now - t < self._period]
            if len(self._calls) >= self._max_calls:
                sleep_for = self._period - (now - self._calls[0])
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
            self._calls.append(time.monotonic())
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None


# ── Timestamp helpers ─────────────────────────────────────────────────

def utc_now() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 string into a timezone-aware datetime."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def time_ago(dt: datetime) -> str:
    """Human-readable 'X ago' string from a datetime to now."""
    delta = utc_now() - dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    return f"{days}d ago"

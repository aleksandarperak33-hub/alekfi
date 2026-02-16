"""Universal LLM client wrapper using OpenAI-compatible endpoints.

Works with DeepSeek, OpenAI, Gemini (via OpenAI compat), Anthropic
(via proxy), and MiniMax — you just swap the base_url and model.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from openai import AsyncOpenAI

from alekfi.config import get_settings

logger = logging.getLogger(__name__)


class LLMClient:
    """Thin async wrapper around any OpenAI-compatible chat completions API."""

    def __init__(
        self,
        provider: str,
        api_key: str,
        base_url: str,
        model: str,
        max_retries: int = 3,
        backoff_base: float = 2.0,
    ) -> None:
        self.provider = provider
        self.model = model
        self.max_retries = max_retries
        self.backoff_base = backoff_base

        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url,
        )

        self._total_prompt_tokens = 0
        self._total_completion_tokens = 0

    # ── core completion ────────────────────────────────────────────────
    async def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        json_mode: bool = False,
    ) -> str:
        """Send a single chat completion request with retry + backoff."""
        messages: list[dict[str, str]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        kwargs: dict[str, Any] = {"model": self.model, "messages": messages}
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        last_exc: BaseException | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = await self._client.chat.completions.create(**kwargs)
                usage = response.usage
                if usage:
                    self._total_prompt_tokens += usage.prompt_tokens
                    self._total_completion_tokens += usage.completion_tokens
                    logger.debug(
                        "LLM usage [%s/%s] prompt=%d completion=%d",
                        self.provider,
                        self.model,
                        usage.prompt_tokens,
                        usage.completion_tokens,
                    )
                return response.choices[0].message.content or ""
            except Exception as exc:
                last_exc = exc
                wait = self.backoff_base ** attempt
                logger.warning(
                    "LLM call failed (attempt %d/%d, provider=%s): %s — retrying in %.1fs",
                    attempt,
                    self.max_retries,
                    self.provider,
                    exc,
                    wait,
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(wait)

        raise RuntimeError(
            f"LLM call failed after {self.max_retries} attempts: {last_exc}"
        ) from last_exc

    # ── batch helper ───────────────────────────────────────────────────
    async def batch_complete(
        self,
        system_prompt: str,
        messages: list[str],
        json_mode: bool = False,
    ) -> list[str]:
        """Run multiple user messages against the same system prompt concurrently."""
        tasks = [
            self.complete(system_prompt, msg, json_mode=json_mode)
            for msg in messages
        ]
        return list(await asyncio.gather(*tasks))

    # ── stats ──────────────────────────────────────────────────────────
    @property
    def token_usage(self) -> dict[str, int]:
        return {
            "prompt_tokens": self._total_prompt_tokens,
            "completion_tokens": self._total_completion_tokens,
            "total_tokens": self._total_prompt_tokens + self._total_completion_tokens,
        }


# ── module-level factory functions ─────────────────────────────────────

_gatekeeper_client: LLMClient | None = None
_brain_client: LLMClient | None = None


def get_gatekeeper_client() -> LLMClient:
    """Return (and cache) an LLMClient configured for the Gatekeeper tier."""
    global _gatekeeper_client
    if _gatekeeper_client is None:
        s = get_settings()
        _gatekeeper_client = LLMClient(
            provider=s.gatekeeper_provider,
            api_key=s.gatekeeper_api_key,
            base_url=s.gatekeeper_base_url,
            model=s.gatekeeper_model,
        )
    return _gatekeeper_client


def get_brain_client() -> LLMClient:
    """Return (and cache) an LLMClient configured for the Brain tier."""
    global _brain_client
    if _brain_client is None:
        s = get_settings()
        _brain_client = LLMClient(
            provider=s.brain_provider,
            api_key=s.brain_api_key,
            base_url=s.brain_base_url,
            model=s.brain_model,
        )
    return _brain_client

"""Tier 2 — Gatekeeper. LLM-powered relevance filter that kills 85-90% of noise."""

from alekfi.gatekeeper.gatekeeper import GatekeeperProcessor
from alekfi.gatekeeper.mock import MockGatekeeper

__all__ = ["GatekeeperProcessor", "MockGatekeeper"]

"""Tier 2 — Gatekeeper. LLM-powered relevance filter that kills 85-90% of noise."""

from open_claw.gatekeeper.gatekeeper import GatekeeperProcessor
from open_claw.gatekeeper.mock import MockGatekeeper

__all__ = ["GatekeeperProcessor", "MockGatekeeper"]

"""Trade-ready research bundle for each Signal.

Goal: turn a noisy cluster of posts into a structured, auditable forecast object:
- normalized claim
- evidence graph + independence scoring
- probabilistic forecast by horizon (calibrated-ish)
- tradability profile
- learned score (expected value proxy)

This is intentionally lightweight and deterministic. It does not call LLMs.
"""

from __future__ import annotations

import math
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from alekfi.marketdata import MarketDataGateway

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:
        return None


def _jaccard(a: str, b: str) -> float:
    a = (a or "").lower()
    b = (b or "").lower()
    sa = {t for t in a.split() if len(t) > 2}
    sb = {t for t in b.split() if len(t) > 2}
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / max(union, 1)


@dataclass
class EvidenceFeatures:
    independence_score: float
    unique_platforms: int
    unique_domains: int
    unique_authors: int
    echo_ratio: float
    origin_time: str | None


async def build_research_bundle(
    *,
    redis,
    signal_dict: dict[str, Any],
    source_posts: list[dict[str, Any]],
    clusters: list[dict[str, Any]] | None,
    tier: str,
    total_score: int,
    sig_type: str,
    primary_sym: str,
    direction: str,
    conviction: float,
    cluster_convergence: float,
    source_count: int,
    platform_count: int,
    source_event_time: datetime | None,
) -> dict[str, Any]:
    now = _utcnow()
    evidence = _build_evidence_graph(source_posts, source_event_time)
    tradability = await _build_tradability(redis, primary_sym, now)
    empirical_priors = await _load_empirical_priors(redis)

    # Calibration prior: combine model conviction and empirical type accuracy (if present).
    type_acc = None
    try:
        type_acc = (signal_dict.get("_type_accuracy") if isinstance(signal_dict, dict) else None)
        type_acc = _safe_float(type_acc, default=float("nan"))
        if math.isnan(type_acc):
            type_acc = None
    except Exception:
        type_acc = None

    forecast = _build_forecast(
        sig_type=sig_type,
        direction=direction,
        conviction=conviction,
        type_accuracy=type_acc,
        cluster_convergence=cluster_convergence,
        independence_score=evidence["independence_score"],
        empirical_priors=empirical_priors,
    )
    forecast["novelty_score"] = round(
        _clamp(
            _safe_float(signal_dict.get("novelty_score"), 0.5 + 0.4 * _clamp(cluster_convergence, 0.0, 1.0)),
            0.0,
            1.0,
        ),
        3,
    )

    learned = _build_learned_score(
        forecast=forecast,
        tradability=tradability,
        tier=tier,
        total_score=total_score,
    )

    claim = _build_normalized_claim(
        signal_dict=signal_dict,
        clusters=clusters or [],
        primary_sym=primary_sym,
        sig_type=sig_type,
        direction=direction,
        created_at=now,
        source_event_time=source_event_time,
        evidence=evidence,
    )

    controls = _build_controls(
        tier=tier,
        evidence=evidence,
        tradability=tradability,
        forecast=forecast,
        source_count=source_count,
        platform_count=platform_count,
    )

    return {
        "claim": claim,
        "evidence": evidence,
        "forecast": forecast,
        "tradability": tradability,
        "learned": learned,
        "controls": controls,
    }


async def _load_empirical_priors(redis) -> dict[str, Any] | None:
    """Load lightweight trained priors from Redis if available."""
    if redis is None:
        return None
    try:
        raw = await redis.get("alekfi:forecast:model:v1")
        if not raw:
            return None
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _build_evidence_graph(source_posts: list[dict[str, Any]], source_event_time: datetime | None) -> dict[str, Any]:
    # Nodes
    nodes: list[dict[str, Any]] = []
    domains: list[str] = []
    authors: list[str] = []
    platforms: list[str] = []
    origin_time = None

    for sp in source_posts or []:
        pid = sp.get("id") or sp.get("post_id") or sp.get("rid")
        platform = (sp.get("platform") or sp.get("p") or "unknown").lower()
        author = (sp.get("author") or sp.get("a") or "unknown").strip()
        url = sp.get("url") or sp.get("u")
        snippet = sp.get("content_snippet") or sp.get("s") or ""
        published_at = sp.get("source_published_at") or sp.get("published_at")

        dom = _domain(url)
        if dom:
            domains.append(dom)
        if author:
            authors.append(author)
        if platform:
            platforms.append(platform)

        if published_at and (origin_time is None or published_at < origin_time):
            origin_time = published_at

        nodes.append(
            {
                "id": str(pid) if pid is not None else None,
                "platform": platform,
                "author": author,
                "domain": dom,
                "url": url,
                "snippet": snippet[:240],
                "published_at": published_at,
            }
        )

    uniq_domains = len({d for d in domains if d})
    uniq_authors = len({a for a in authors if a})
    uniq_platforms = len({p for p in platforms if p})

    # Echo and origin overlap proxies.
    echo_ratio = 0.0
    same_origin_pairs = 0
    total_pairs = 0
    if nodes:
        from collections import Counter

        dom_counts = Counter(n.get("domain") for n in nodes if n.get("domain"))
        auth_counts = Counter(n.get("author") for n in nodes if n.get("author"))
        dom_top = dom_counts.most_common(1)[0][1] if dom_counts else 0
        auth_top = auth_counts.most_common(1)[0][1] if auth_counts else 0
        echo_ratio = max(dom_top, auth_top) / max(len(nodes), 1)

        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                total_pairs += 1
                ni, nj = nodes[i], nodes[j]
                if ni.get("domain") and ni.get("domain") == nj.get("domain"):
                    same_origin_pairs += 1

    same_origin_ratio = (same_origin_pairs / max(total_pairs, 1)) if total_pairs else 0.0

    # Modality diversity boosts independence (social + official + market/quant signals).
    def _modality(p: str) -> str:
        p = (p or "").lower()
        if p in {"sec_edgar", "sec_filings", "government", "patent", "congressional_trades"}:
            return "official"
        if p in {"news_rss", "news", "finviz_news"}:
            return "news"
        if p in {"market_context", "options_flow", "short_interest", "commodities"}:
            return "market_metric"
        return "social"

    modalities = {_modality(p) for p in platforms if p}
    modality_bonus = 0.12 * min(max(len(modalities) - 1, 0), 3)

    # Time separation bonus: confirmations that are not posted at exactly the same moment.
    published = []
    for n in nodes:
        p = n.get("published_at")
        if isinstance(p, str):
            try:
                published.append(datetime.fromisoformat(p.replace("Z", "+00:00")))
            except Exception:
                continue
    time_separation_bonus = 0.0
    if len(published) >= 2:
        span_h = abs((max(published) - min(published)).total_seconds()) / 3600.0
        time_separation_bonus = _clamp(span_h / 12.0, 0.0, 1.0) * 0.08

    origin_diversity = _clamp((uniq_domains / max(len(nodes), 1)) * 2.0, 0.0, 1.0)
    author_diversity = _clamp((uniq_authors / max(len(nodes), 1)) * 2.0, 0.0, 1.0)
    echo_penalty = _clamp(0.55 * echo_ratio + 0.45 * same_origin_ratio, 0.0, 1.0) * 0.45

    raw = 0.0
    raw += 0.28 * _clamp(uniq_platforms / 3.0, 0.0, 1.0)
    raw += 0.22 * origin_diversity
    raw += 0.18 * author_diversity
    raw += modality_bonus
    raw += time_separation_bonus
    raw -= echo_penalty

    independence = _clamp(raw, 0.0, 1.0)

    # Simple edge list for audit (keep small)
    edges: list[dict[str, Any]] = []
    for i in range(min(len(nodes), 25)):
        for j in range(i + 1, min(len(nodes), 25)):
            a = nodes[i]
            b = nodes[j]
            if a.get("domain") and a.get("domain") == b.get("domain"):
                edges.append({"src": a.get("id"), "dst": b.get("id"), "type": "same_domain"})
            if a.get("author") and a.get("author") == b.get("author"):
                edges.append({"src": a.get("id"), "dst": b.get("id"), "type": "same_author"})
            if _jaccard(a.get("snippet") or "", b.get("snippet") or "") >= 0.75:
                edges.append({"src": a.get("id"), "dst": b.get("id"), "type": "near_duplicate"})
            if len(edges) >= 60:
                break
        if len(edges) >= 60:
            break

    # Prefer computed earliest published_at; fall back to source_event_time.
    origin_time_out = origin_time
    if not origin_time_out and source_event_time:
        origin_time_out = source_event_time.isoformat()

    verification_hits: list[dict[str, Any]] = []
    for n in nodes:
        dom = (n.get("domain") or "").lower()
        plat = (n.get("platform") or "").lower()
        if dom.endswith("sec.gov") or plat in {"sec_edgar", "sec_filings"}:
            verification_hits.append({"type": "regulatory_filing", "evidence_id": n.get("id"), "strength": "high"})
        elif "status" in dom or "downdetector" in dom:
            verification_hits.append({"type": "outage_signal", "evidence_id": n.get("id"), "strength": "medium"})
        elif plat in {"news_rss", "finviz_news"} and dom:
            verification_hits.append({"type": "independent_news", "evidence_id": n.get("id"), "strength": "medium"})

    source_credibility = 0.25 + 0.20 * min(uniq_domains, 4) / 4.0 + 0.25 * min(uniq_platforms, 3) / 3.0
    if any(vh["strength"] == "high" for vh in verification_hits):
        source_credibility += 0.2
    source_credibility = _clamp(source_credibility, 0.0, 1.0)

    return {
        "independence_score": round(independence, 3),
        "unique_platforms": uniq_platforms,
        "unique_domains": uniq_domains,
        "unique_authors": uniq_authors,
        "echo_ratio": round(echo_ratio, 3),
        "independence_breakdown": {
            "echo_penalty": round(echo_penalty, 3),
            "origin_diversity": round(origin_diversity, 3),
            "modality_bonus": round(modality_bonus, 3),
            "time_separation_bonus": round(time_separation_bonus, 3),
            "same_origin_ratio": round(same_origin_ratio, 3),
        },
        "source_credibility": round(source_credibility, 3),
        "verification_hits": verification_hits[:8],
        "origin_time": origin_time_out,
        "nodes": nodes[:25],
        "edges": edges,
    }


async def _build_tradability(redis, primary_sym: str, now: datetime) -> dict[str, Any]:
    md = None
    if primary_sym:
        try:
            gw = MarketDataGateway(redis_client=redis) if redis is not None else MarketDataGateway()
            md = await gw.get_market_data(primary_sym, fail_closed=False)
        except Exception:
            md = None

    price = md.last_price if md else None
    volume = md.volume if md else None
    change_1d = md.ret_1d if md else None
    change_5d = md.ret_5d if md else None
    dollar_vol = md.dollar_volume if md else None
    spread_bps = md.spread_bps_est if md else 50.0

    # Capacity proxy: assume 1% of daily dollar volume is tradable.
    capacity_usd = min(dollar_vol * 0.01, 250_000.0) if dollar_vol is not None else None

    tradable = True
    reasons: list[str] = []
    if price is not None and price < 1.0:
        tradable = False
        reasons.append("penny_stock")
    if dollar_vol is not None and dollar_vol < 3_000_000:
        tradable = False
        reasons.append("low_liquidity")
    if md is None or price is None or volume is None:
        tradable = False
        reasons.append("missing_market_data")
    if md is not None:
        qf = set(md.quality_flags or [])
        if "AUTH_FAIL" in qf:
            tradable = False
            reasons.append("market_data_auth_fail")
        if "SYMBOL_INVALID" in qf:
            tradable = False
            reasons.append("symbol_invalid")

    return {
        "pass": bool(tradable),
        "reasons": reasons,
        "primary_symbol": primary_sym,
        "price": round(float(price), 4) if price is not None else None,
        "volume": float(volume) if volume is not None else None,
        "dollar_volume_est": round(float(dollar_vol), 2) if dollar_vol is not None else None,
        "spread_bps_est": round(float(spread_bps), 1),
        "capacity_usd_est": round(float(capacity_usd), 2) if capacity_usd is not None else None,
        "change_1d": round(float(change_1d), 3) if change_1d is not None else None,
        "change_5d": round(float(change_5d), 3) if change_5d is not None else None,
        "atr_14": round(float(md.atr_14), 4) if md and md.atr_14 is not None else None,
        "market_data_quality_flags": sorted(set(md.quality_flags)) if md else ["NO_DATA"],
        "market_data_provider": md.provider_used if md else None,
        "asof": now.isoformat(),
    }


def _build_forecast(
    *,
    sig_type: str,
    direction: str,
    conviction: float,
    type_accuracy: float | None,
    cluster_convergence: float,
    independence_score: float,
    empirical_priors: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Convert a "conviction" number into calibrated-ish probabilities.
    # This is a placeholder until the dedicated labeler + model is wired in.

    # Prior from historical accuracy (if known)
    prior = type_accuracy if type_accuracy is not None else 0.52
    base = 0.55 * _clamp(conviction, 0.0, 1.0) + 0.45 * _clamp(prior, 0.05, 0.95)

    # Evidence modifiers
    base *= (0.60 + 0.40 * _clamp(cluster_convergence, 0.0, 1.0))
    base *= (0.70 + 0.30 * _clamp(independence_score, 0.0, 1.0))

    p_correct = _clamp(base, 0.05, 0.95)

    # Horizon curves: longer horizons should (slightly) increase hit-rate, but also costs.
    p = {
        "1h": _clamp(p_correct - 0.10, 0.05, 0.90),
        "4h": _clamp(p_correct - 0.05, 0.05, 0.92),
        "1d": _clamp(p_correct, 0.05, 0.94),
        "3d": _clamp(p_correct + 0.03, 0.05, 0.95),
        "7d": _clamp(p_correct + 0.05, 0.05, 0.95),
    }

    # Expected move magnitudes are placeholders; later replaced by labeler-trained estimates.
    # Expressed as percent move of the primary instrument.
    move = {
        "1h": 0.6,
        "4h": 1.0,
        "1d": 1.6,
        "3d": 2.4,
        "7d": 3.5,
    }
    adverse = {
        "1h": 0.4,
        "4h": 0.7,
        "1d": 1.1,
        "3d": 1.7,
        "7d": 2.6,
    }

    # Map to direction distribution.
    d = (direction or "LONG").upper()
    out: dict[str, Any] = {"model": "v0_heuristic", "signal_type": sig_type, "by_horizon": {}}

    # Optional learned priors from offline training pipeline.
    priors = {}
    global_priors = {}
    if isinstance(empirical_priors, dict):
        priors = (((empirical_priors.get("by_signal_type") or {}).get(sig_type)) or {}).get("horizons") or {}
        global_priors = ((empirical_priors.get("global") or {}).get("horizons") or {})
        if priors:
            out["model"] = str(empirical_priors.get("model_name") or "v1_empirical_priors")
        elif global_priors:
            out["model"] = str(empirical_priors.get("model_name") or "v1_empirical_priors_global")

    for h in ("1h", "4h", "1d", "3d", "7d"):
        prior_h = priors.get(h) if isinstance(priors, dict) else None
        if not isinstance(prior_h, dict) and isinstance(global_priors, dict):
            prior_h = global_priors.get(h)
        prior_p = _safe_float((prior_h or {}).get("p_correct"), default=None)
        ph = p[h]
        if prior_p is not None:
            # Blend model prior with signal-specific conviction/evidence.
            ph = _clamp(0.65 * ph + 0.35 * prior_p, 0.05, 0.95)
        move_h = _safe_float((prior_h or {}).get("exp_favorable_move_pct"), default=move[h])
        adv_h = _safe_float((prior_h or {}).get("exp_adverse_move_pct"), default=adverse[h])
        if d == "LONG":
            p_up, p_down, p_flat = ph, 1.0 - ph, 0.0
        elif d == "SHORT":
            p_up, p_down, p_flat = 1.0 - ph, ph, 0.0
        elif d == "HEDGE":
            p_up, p_down, p_flat = (1.0 - ph) / 2.0, (1.0 - ph) / 2.0, ph
        else:
            # NO_TRADE
            p_up, p_down, p_flat = 0.33, 0.33, 0.34

        out["by_horizon"][h] = {
            "p_up": round(p_up, 3),
            "p_down": round(p_down, 3),
            "p_flat": round(p_flat, 3),
            "expected_move_pct": round(move_h, 3),
            "expected_adverse_pct": round(adv_h, 3),
            "expected_time_to_move_hours": {"1h": 0.6, "4h": 2.0, "1d": 8.0, "3d": 24.0, "7d": 72.0}[h],
        }

    return out


def _build_learned_score(*, forecast: dict[str, Any], tradability: dict[str, Any], tier: str, total_score: int) -> dict[str, Any]:
    cost_bps = _safe_float(tradability.get("spread_bps_est"), 25.0) * 1.5
    cost_pct = cost_bps / 10000.0 * 100.0
    by_horizon = forecast.get("by_horizon") or {}
    ev_by_h: dict[str, float] = {}
    best_h = "1d"
    best_ev = -999.0
    for h, fh in by_horizon.items():
        p_up = _safe_float(fh.get("p_up"), 0.0)
        p_down = _safe_float(fh.get("p_down"), 0.0)
        exp = _safe_float(fh.get("expected_move_pct"), 0.0)
        adv = _safe_float(fh.get("expected_adverse_pct"), 0.0)
        ev = p_up * exp - p_down * adv - cost_pct
        ev_by_h[h] = round(ev, 4)
        if ev > best_ev:
            best_ev = ev
            best_h = h

    # Convert to a 0-100 score for ranking.
    score = 50.0
    score += best_ev * 10.0
    score += (total_score - 50) * 0.15
    if tier in ("CRITICAL", "HIGH"):
        score += 5.0

    return {
        "expected_pnl_pct_1d": ev_by_h.get("1d"),
        "expected_pnl_pct_by_horizon": ev_by_h,
        "best_horizon": best_h,
        "best_expected_pnl_pct": round(best_ev, 4),
        "rank_score": round(_clamp(score, 0.0, 100.0), 2),
        "cost_bps_est": round(cost_bps, 1),
    }


def _build_normalized_claim(
    *,
    signal_dict: dict[str, Any],
    clusters: list[dict[str, Any]],
    primary_sym: str,
    sig_type: str,
    direction: str,
    created_at: datetime,
    source_event_time: datetime | None,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    # Best-effort canonical event frame.
    headline = (signal_dict.get("headline") or "").strip()
    thesis = (signal_dict.get("thesis") or "").strip()
    what = headline or (thesis.split(".")[0][:180] if thesis else sig_type)

    # Try to infer event_type from the top cluster.
    event_type = None
    for c in clusters or []:
        if (c.get("primary_ticker") or c.get("ticker")) == primary_sym:
            event_type = c.get("event_type")
            break
    if not event_type and clusters:
        event_type = clusters[0].get("event_type")

    when = source_event_time.isoformat() if source_event_time else (evidence.get("origin_time") or created_at.isoformat())

    instruments = signal_dict.get("affected_instruments") or []
    who = []
    for inst in instruments:
        sym = inst.get("symbol")
        if sym:
            who.append(sym)

    return {
        "what": what,
        "who": who or ([primary_sym] if primary_sym else []),
        "when": when,
        "event_type": event_type or sig_type,
        "direction": (direction or "LONG").upper(),
    }


def _build_controls(
    *,
    tier: str,
    evidence: dict[str, Any],
    tradability: dict[str, Any],
    forecast: dict[str, Any],
    source_count: int,
    platform_count: int,
) -> dict[str, Any]:
    # Hard gates for top tiers.
    independence = _safe_float(evidence.get("independence_score"), 0.0)
    source_credibility = _safe_float(evidence.get("source_credibility"), 0.0)
    verification_hits = evidence.get("verification_hits") or []
    tradable = bool(tradability.get("pass"))
    novelty = _safe_float(forecast.get("novelty_score"), 0.0)

    hard_fail = []
    if tier in ("CRITICAL", "HIGH"):
        if independence < 0.45:
            hard_fail.append("low_independence")
        if not tradable:
            hard_fail.append("untradable")
        if source_count < 2:
            hard_fail.append("too_few_sources")
        if platform_count < 2:
            # Exception: verified single-modality claim with high credibility + novelty.
            verified_single = (
                len(verification_hits) >= 1
                and source_credibility >= 0.70
                and novelty >= 0.60
            )
            if not verified_single:
                hard_fail.append("not_cross_platform")

    return {
        "hard_fail": hard_fail,
        "pass": len(hard_fail) == 0,
        "min_independence_for_top": 0.45,
        "verified_single_modality_ok": (
            len(verification_hits) >= 1 and source_credibility >= 0.70 and novelty >= 0.60
        ),
    }

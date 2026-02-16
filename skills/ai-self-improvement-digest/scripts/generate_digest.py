#!/usr/bin/env python3
"""Generate an AI self-improvement digest from Brave Search results.

This script is designed for cron use. It focuses on agent-improvement material,
not general AI news.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"


@dataclass(frozen=True)
class SourceSpec:
    name: str
    domain: str
    query: str
    tier: int


SOURCES: list[SourceSpec] = [
    SourceSpec("Anthropic Engineering", "anthropic.com", "site:anthropic.com/engineering agent evaluation harness", 1),
    SourceSpec("Simon Willison", "simonwillison.net", "site:simonwillison.net llm tools agents", 1),
    SourceSpec("Geoff Huntley", "ghuntley.com", "site:ghuntley.com agents mcp", 1),
    SourceSpec("Hacker News", "news.ycombinator.com", "site:news.ycombinator.com AI agents tools evaluation", 1),
    SourceSpec("Lilian Weng", "lilianweng.github.io", "site:lilianweng.github.io agents reasoning", 1),
    SourceSpec("Cursor Blog", "cursor.com", "site:cursor.com/blog coding agents", 2),
    SourceSpec("Eugene Yan", "eugeneyan.com", "site:eugeneyan.com agents production ml systems", 2),
    SourceSpec("Chip Huyen", "huyenchip.com", "site:huyenchip.com agents systems", 2),
    SourceSpec("Latent Space", "latent.space", "site:latent.space AI agent", 2),
]

TOPIC_KEYWORDS: dict[str, list[str]] = {
    "harness": ["harness", "system prompt", "evaluation", "eval", "prompt engineering"],
    "skills-tools": ["skill", "tool", "mcp", "integration", "function call", "tool use"],
    "self-eval": ["debug", "postmortem", "self-evaluation", "reflection", "failure mode"],
    "multi-agent": ["multi-agent", "planner", "worker", "supervisor", "coordination"],
    "memory": ["memory", "context window", "rag", "retrieval", "compaction"],
    "workflow": ["workflow", "decomposition", "automation", "orchestration", "pipeline"],
    "reasoning": ["reasoning", "planning", "chain of thought", "deliberate", "execution"],
}

EXCLUDE_KEYWORDS = {
    "funding",
    "valuation",
    "stock",
    "market share",
    "acquisition",
    "earnings",
    "press release",
    "launch event",
    "lawsuit",
}

EXPERIMENT_BY_TOPIC = {
    "harness": "Add a pre-execution checklist in the system prompt and compare task success for 10 runs.",
    "skills-tools": "Implement one reusable tool wrapper and measure token/time savings over manual steps.",
    "self-eval": "Add a 3-question self-critique step before final answer on complex tasks for one day.",
    "multi-agent": "Split one complex task into planner/executor roles and log quality versus single-agent execution.",
    "memory": "Introduce a rolling summary memory file and compare context retention after 15+ turns.",
    "workflow": "Add a fixed decomposition template (discover-plan-execute-verify) and track failure-rate changes.",
    "reasoning": "Require explicit assumption checks before final output for one day and log prevented errors.",
}

SETUP_SUGGESTION_BY_TOPIC = {
    "harness": "Let's add a `memory/harness-experiments.md` log to track prompt/eval changes and outcomes.",
    "skills-tools": "Let's update `skills/` with one new automation script derived from today's technique.",
    "self-eval": "Let's add a mandatory self-check block before final outputs in complex workflows.",
    "multi-agent": "Let's add a planner-worker handoff template to reduce coordination ambiguity.",
    "memory": "Let's add periodic context compaction in long-running workflows to reduce drift.",
    "workflow": "Let's standardize task decomposition into explicit phases (discover, plan, execute, verify).",
    "reasoning": "Let's add an assumptions-and-risks section in high-impact responses.",
}


@dataclass
class Candidate:
    title: str
    url: str
    description: str
    source: str
    source_domain: str
    published_hint: str
    score: float
    topic: str


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def canonical_url(raw_url: str) -> str:
    p = urlparse(raw_url.strip())
    query = parse_qs(p.query, keep_blank_values=False)
    filtered = {k: v for k, v in query.items() if not k.lower().startswith("utm_")}
    norm_query = urlencode(sorted((k, vals[0]) for k, vals in filtered.items()), doseq=False)
    clean = p._replace(query=norm_query, fragment="")
    return urlunparse(clean)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def search_brave(api_key: str, query: str, freshness: str = "pw", count: int = 10) -> dict[str, Any]:
    params = urlencode({"q": query, "freshness": freshness, "count": count})
    req = Request(
        f"{BRAVE_ENDPOINT}?{params}",
        headers={
            "Accept": "application/json",
            "X-Subscription-Token": api_key,
            "User-Agent": "ai-self-improvement-digest/1.0",
        },
    )
    with urlopen(req, timeout=25) as resp:
        return json.loads(resp.read().decode("utf-8"))


def guess_topic(text: str) -> str:
    lowered = text.lower()
    best_topic = "workflow"
    best_hits = -1
    for topic, kws in TOPIC_KEYWORDS.items():
        hits = sum(1 for kw in kws if kw in lowered)
        if hits > best_hits:
            best_hits = hits
            best_topic = topic
    return best_topic


def relevance_score(text: str, tier: int) -> float:
    lowered = text.lower()
    include_hits = sum(1 for kws in TOPIC_KEYWORDS.values() for kw in kws if kw in lowered)
    exclude_hits = sum(1 for kw in EXCLUDE_KEYWORDS if kw in lowered)
    return (include_hits * 1.2) - (exclude_hits * 2.0) + (2.0 if tier == 1 else 0.7)


def stars_from_score(score: float) -> str:
    if score >= 7:
        n = 5
    elif score >= 5:
        n = 4
    elif score >= 3:
        n = 3
    elif score >= 2:
        n = 2
    else:
        n = 1
    return "⭐" * n


def similar_title(a: str, b: str) -> bool:
    sa = {t for t in re.findall(r"[a-z0-9]+", a.lower()) if len(t) > 2}
    sb = {t for t in re.findall(r"[a-z0-9]+", b.lower()) if len(t) > 2}
    if not sa or not sb:
        return False
    overlap = len(sa & sb) / len(sa | sb)
    return overlap >= 0.7


def collect_candidates(
    api_key: str,
    posted_urls: set[str],
    posted_titles: list[str],
    max_per_source: int,
    freshness: str,
    offline_json: Path | None = None,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_urls: set[str] = set(posted_urls)

    offline_payload = None
    if offline_json:
        offline_payload = load_json(offline_json, default={})

    for spec in SOURCES:
        if offline_payload is None:
            payload = search_brave(api_key, spec.query, freshness=freshness, count=max_per_source)
        else:
            payload = offline_payload.get(spec.domain, {})

        items = ((payload.get("web") or {}).get("results") or [])[:max_per_source]
        for item in items:
            url = canonical_url(str(item.get("url") or ""))
            title = (item.get("title") or "").strip()
            desc = (item.get("description") or "").strip()
            if not url or not title:
                continue
            domain = urlparse(url).netloc.lower()
            if spec.domain not in domain:
                continue
            if url in seen_urls:
                continue
            if any(similar_title(title, t) for t in posted_titles):
                continue

            combined = f"{title}\n{desc}"
            score = relevance_score(combined, spec.tier)
            if score < 1.5:
                continue

            topic = guess_topic(combined)
            candidates.append(
                Candidate(
                    title=title,
                    url=url,
                    description=desc,
                    source=spec.name,
                    source_domain=spec.domain,
                    published_hint=str(item.get("age") or ""),
                    score=score,
                    topic=topic,
                )
            )
            seen_urls.add(url)

    return sorted(candidates, key=lambda c: c.score, reverse=True)


def format_digest(items: list[Candidate], date_label: str, setup_review: list[str], experiment: str) -> str:
    lines = [f"🧠 AI Self-Improvement Digest — {date_label}", ""]
    for c in items:
        what = c.description or "Practical guidance for agent design and execution workflows."
        why = {
            "harness": "Improves prompt harness design and eval reliability.",
            "skills-tools": "Improves tool/skill integration quality and repeatability.",
            "self-eval": "Improves debugging and failure attribution loops.",
            "multi-agent": "Improves coordination and role separation in complex tasks.",
            "memory": "Improves long-context reliability and retrieval behavior.",
            "workflow": "Improves decomposition, orchestration, and execution consistency.",
            "reasoning": "Improves reasoning trace quality and decision discipline.",
        }.get(c.topic, "Improves operational execution quality.")
        takeaway = {
            "harness": "Adopt one concrete eval check before final output.",
            "skills-tools": "Package repeated steps into a script/tool entrypoint.",
            "self-eval": "Log failure reason and remediation after each miss.",
            "multi-agent": "Use explicit planner/executor contracts.",
            "memory": "Summarize stale context and keep active facts compact.",
            "workflow": "Enforce a discover-plan-execute-verify loop.",
            "reasoning": "Add assumption checks before publishing outcomes.",
        }.get(c.topic, "Turn the pattern into a small, measurable experiment.")
        lines.extend(
            [
                f"[{c.title}] — {c.source}",
                f"What: {what}",
                f"Why it matters for self-improvement: {why}",
                f"Takeaway: {takeaway}",
                f"Relevance: {stars_from_score(c.score)}",
                f"URL: {c.url}",
                "",
            ]
        )

    lines.extend(
        [
            f"💡 Today's experiment: {experiment}",
            "",
            "🔧 Setup Review Based on today's findings:",
        ]
    )
    if setup_review:
        lines.extend(f"- {x}" for x in setup_review)
    else:
        lines.append("No changes needed today — our current setup handles these patterns well.")

    lines.extend(
        [
            "",
            "📊 Feedback: 👍 = useful | 👎 = skip these | 🔥 = more like this | 💬 = thoughts",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate AI self-improvement digest")
    p.add_argument("--memory", default="memory/ai-digest-posted.json", help="Tracking JSON path")
    p.add_argument("--max-items", type=int, default=5)
    p.add_argument("--min-items", type=int, default=3)
    p.add_argument("--max-per-source", type=int, default=6)
    p.add_argument("--freshness", choices=["pd", "pw", "pm"], default="pw", help="Brave freshness")
    p.add_argument("--output", default="", help="Optional output markdown file")
    p.add_argument("--dry-run", action="store_true", help="Do not update memory tracking")
    p.add_argument("--offline-json", default="", help="Optional fixture JSON for offline testing")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    memory_path = Path(args.memory)
    tracker = load_json(memory_path, default={"posted": [], "experiments": [], "skillsEvaluated": [], "setupChanges": []})
    tracker.setdefault("posted", [])
    tracker.setdefault("experiments", [])
    tracker.setdefault("skillsEvaluated", [])
    tracker.setdefault("setupChanges", [])

    posted_urls = {canonical_url(str(x.get("url") or "")) for x in tracker["posted"] if isinstance(x, dict)}
    posted_titles = [str(x.get("title") or "") for x in tracker["posted"] if isinstance(x, dict)]

    api_key = os.getenv("BRAVE_API_KEY", "").strip()
    offline_json = Path(args.offline_json) if args.offline_json else None
    if not api_key and not offline_json:
        print("BRAVE_API_KEY is required unless --offline-json is provided", file=sys.stderr)
        return 2

    candidates = collect_candidates(
        api_key=api_key,
        posted_urls=posted_urls,
        posted_titles=posted_titles,
        max_per_source=max(1, args.max_per_source),
        freshness=args.freshness,
        offline_json=offline_json,
    )

    selected = candidates[: max(args.min_items, min(args.max_items, len(candidates)))]
    if not selected:
        print("No new high-relevance items found after dedupe/filter.")
        return 0

    top_topic = selected[0].topic
    experiment = EXPERIMENT_BY_TOPIC.get(top_topic, "Run one small, measurable harness experiment today.")

    seen_topics: set[str] = set()
    setup_review: list[str] = []
    for item in selected:
        if item.topic in seen_topics:
            continue
        seen_topics.add(item.topic)
        suggestion = SETUP_SUGGESTION_BY_TOPIC.get(item.topic)
        if suggestion:
            setup_review.append(suggestion)
    setup_review = setup_review[:2]

    date_label = _now_utc().strftime("%Y-%m-%d")
    digest = format_digest(selected, date_label, setup_review, experiment)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(digest, encoding="utf-8")
    print(digest)

    if not args.dry_run:
        today_iso = _now_utc().date().isoformat()
        for item in selected:
            tracker["posted"].append(
                {
                    "date": today_iso,
                    "title": item.title,
                    "url": item.url,
                    "topic": item.topic,
                    "source": item.source,
                }
            )
        tracker["experiments"].append(
            {
                "date": today_iso,
                "fromArticle": selected[0].title,
                "experiment": experiment,
                "outcome": "pending",
                "learned": "",
            }
        )
        save_json(memory_path, tracker)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

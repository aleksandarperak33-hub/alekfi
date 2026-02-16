from __future__ import annotations

from alekfi.workers.corroboration_worker import CorroborationWorker


def test_corroboration_evidence_tracks_diversity_and_verification() -> None:
    worker = object.__new__(CorroborationWorker)
    source_posts = [
        {
            "id": "1",
            "platform": "reddit",
            "author": "alice",
            "url": "https://www.reddit.com/r/stocks/comments/abc",
            "content_snippet": "AAPL outage and app complaints",
        },
        {
            "id": "2",
            "platform": "x",
            "author": "bob",
            "url": "https://x.com/someone/status/123",
            "content_snippet": "AAPL service outage",
        },
        {
            "id": "3",
            "platform": "sec_filings",
            "author": "sec",
            "url": "https://www.sec.gov/ixviewer/doc",
            "content_snippet": "8-K filing",
        },
    ]

    evidence = CorroborationWorker._compute_evidence(worker, source_posts)
    assert evidence["unique_platforms"] == 3
    assert evidence["unique_domains"] >= 2
    assert evidence["independence_score"] > 0.0
    assert "independence_breakdown" in evidence
    assert any(v["type"] == "regulatory_filing" for v in (evidence.get("verification_hits") or []))


def test_verified_single_modality_exception_keeps_strict_meaningful() -> None:
    worker = object.__new__(CorroborationWorker)
    evidence = {
        "independence_score": 0.41,
        "source_credibility": 0.8,
        "verification_hits": [{"type": "regulatory_filing", "strength": "high"}],
    }

    controls = CorroborationWorker._compute_controls(
        worker,
        tier="HIGH",
        evidence=evidence,
        tradability={"pass": True},
        source_count=2,
        platform_count=1,
        novelty=0.7,
    )

    assert controls["verified_single_modality_ok"] is True
    assert controls["pass"] is True

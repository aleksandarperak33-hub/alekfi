from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "skills" / "ai-self-improvement-digest" / "scripts" / "generate_digest.py"
FIXTURE = REPO_ROOT / "skills" / "ai-self-improvement-digest" / "references" / "fixtures.brave.sample.json"


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )


def test_digest_script_generates_output_and_updates_memory(tmp_path: Path) -> None:
    memory = tmp_path / "ai-digest-posted.json"
    output = tmp_path / "digest.md"

    proc = _run(
        [
            "--offline-json",
            str(FIXTURE),
            "--memory",
            str(memory),
            "--output",
            str(output),
        ]
    )

    assert proc.returncode == 0, proc.stderr
    assert output.exists()

    digest_text = output.read_text(encoding="utf-8")
    assert "AI Self-Improvement Digest" in digest_text
    assert "Today's experiment" in digest_text
    assert "Setup Review" in digest_text

    payload = json.loads(memory.read_text(encoding="utf-8"))
    assert len(payload.get("posted", [])) >= 3
    assert len(payload.get("experiments", [])) >= 1


def test_digest_script_dedupes_on_second_run(tmp_path: Path) -> None:
    memory = tmp_path / "ai-digest-posted.json"
    output = tmp_path / "digest.md"

    first = _run(
        [
            "--offline-json",
            str(FIXTURE),
            "--memory",
            str(memory),
            "--output",
            str(output),
        ]
    )
    assert first.returncode == 0
    first_payload = json.loads(memory.read_text(encoding="utf-8"))
    first_posted_len = len(first_payload.get("posted", []))

    second = _run(
        [
            "--offline-json",
            str(FIXTURE),
            "--memory",
            str(memory),
            "--output",
            str(output),
        ]
    )
    assert second.returncode == 0

    second_payload = json.loads(memory.read_text(encoding="utf-8"))
    second_posted_len = len(second_payload.get("posted", []))

    assert second_posted_len == first_posted_len
    assert "No new high-relevance items found" in second.stdout


def test_canonical_url_strips_tracking_params() -> None:
    spec = importlib.util.spec_from_file_location("digest", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)

    raw = "https://example.com/article?utm_source=x&utm_medium=y&id=42#frag"
    out = mod.canonical_url(raw)
    assert out == "https://example.com/article?id=42"

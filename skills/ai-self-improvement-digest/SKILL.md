---
name: ai-self-improvement-digest
description: Generate a daily digest of AI self-improvement material (not general AI news) focused on harness/system prompt patterns, tool/skill development, self-evaluation/debugging, multi-agent coordination, memory/context management, workflow automation, and reasoning/execution techniques. Use when setting up or running an agent learning loop with deduped sources, experiment suggestions, and setup review actions.
---

# AI Self-Improvement Digest

Use this skill to produce a daily 3-5 item digest that improves agent capability.

## Workflow
1. Ensure tracking file exists:
- `memory/ai-digest-posted.json`
- Initialize from `references/ai-digest-posted.template.json` if missing.

2. Gather content from prioritized sources in `references/source-priority.md`.
- Keep last 24-72h windows.
- Exclude general AI/business/model-release news.

3. Deduplicate before scoring.
- Skip if URL already exists in tracking.
- Skip if topic/title is substantially similar.

4. Keep only agent-improvement content.
- Harness/system prompt engineering
- Skill/tool development
- Self-eval/debugging
- Multi-agent coordination
- Memory/context management
- Workflow automation
- Reasoning/execution patterns

5. Format output exactly:
- `🧠 AI Self-Improvement Digest — YYYY-MM-DD`
- 3-5 entries with `What`, `Why it matters`, `Takeaway`, `Relevance`, `URL`
- `💡 Today's experiment`
- `🔧 Setup Review Based on today's findings`
- `📊 Feedback`

6. Update tracking file with new posted items and today’s experiment.

## Scripted Path (preferred)
Use the bundled script for deterministic output and tracking updates:

```bash
python skills/ai-self-improvement-digest/scripts/generate_digest.py \
  --memory memory/ai-digest-posted.json \
  --output memory/ai-digest-latest.md
```

Required env var:
- `BRAVE_API_KEY`

Useful flags:
- `--dry-run`
- `--freshness pd|pw|pm`
- `--offline-json skills/ai-self-improvement-digest/references/fixtures.brave.sample.json`

## Cron Setup Example
```bash
clawdbot cron add --name ai-self-improvement-digest \
  --schedule "30 8 * * *" \
  --tz "America/New_York" \
  --message "Run scripts/run_ai_self_improvement_digest.sh and post memory/ai-digest-latest.md"
```

Use `references/cron-prompt.md` if you need a richer cron message body.

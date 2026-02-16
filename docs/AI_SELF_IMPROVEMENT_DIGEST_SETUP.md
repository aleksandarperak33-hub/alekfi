# AI Self-Improvement Digest Setup

## 1) Configure Brave Search API
In your Clawdbot config:

```json
{
  "tools": {
    "web": {
      "search": {
        "enabled": true,
        "provider": "brave",
        "apiKey": "YOUR_BRAVE_API_KEY"
      }
    }
  }
}
```

Also set env var for scripted generation:

```bash
export BRAVE_API_KEY="YOUR_BRAVE_API_KEY"
```

## 2) Initialize tracking memory

```bash
mkdir -p memory
cp skills/ai-self-improvement-digest/references/ai-digest-posted.template.json memory/ai-digest-posted.json
```

## 3) Generate digest manually

```bash
python skills/ai-self-improvement-digest/scripts/generate_digest.py \
  --memory memory/ai-digest-posted.json \
  --output memory/ai-digest-latest.md
```

or use the wrapper:

```bash
scripts/run_ai_self_improvement_digest.sh
```

Offline test mode:

```bash
python skills/ai-self-improvement-digest/scripts/generate_digest.py \
  --offline-json skills/ai-self-improvement-digest/references/fixtures.brave.sample.json \
  --memory memory/ai-digest-posted.json \
  --dry-run
```

## 4) Add cron schedule

```bash
clawdbot cron add --name ai-self-improvement-digest \
  --schedule "30 8 * * *" \
  --tz "America/New_York" \
  --message "Run scripts/run_ai_self_improvement_digest.sh and post memory/ai-digest-latest.md"
```

For a richer prompt body, use:
- `skills/ai-self-improvement-digest/references/cron-prompt.md`

## 5) Optional X/Twitter enrichment
If you install `x-research` skill, run those lookups before posting and include any practical implementation threads in the same digest.

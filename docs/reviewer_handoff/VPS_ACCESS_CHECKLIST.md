# VPS Access Checklist (For External Reviewer)

## Minimum Access Needed
- GitHub repo read access.
- SSH read-only shell access to `147.93.116.107` (or supervised session).
- Permission to run non-destructive Docker/log/SQL read commands.

## Suggested Read-Only Command Set
- Service health and logs:
  - `docker ps`
  - `docker logs --since ... <container>`
- Metrics/evaluation:
  - `python3 /docker/alekfi/scripts/metrics_snapshot.py`
  - `docker exec alekfi-brain-1 python -m alekfi.tools.eval_harness ...`
- DB schema/sample reads:
  - `docker exec alekfi-postgres-1 psql ... SELECT ...`
  - `docker exec alekfi-postgres-1 pg_dump -s ...`

## Security / Scope Guardrails
- Do not share raw `.env` values.
- Do not rotate keys in reviewer session.
- Avoid write operations unless explicitly approved.
- Redact sensitive payloads before external sharing.

## Fast Validation Commands
```bash
ssh root@147.93.116.107 "python3 /docker/alekfi/scripts/metrics_snapshot.py"
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20'"
ssh root@147.93.116.107 "/docker/alekfi/scripts/no_regressions.sh"
```

## If You Want a Dedicated Deploy Key
Generate on your machine:
```bash
ssh-keygen -t ed25519 -C "alekfi-reviewer" -f ~/.ssh/alekfi_reviewer -N ""
cat ~/.ssh/alekfi_reviewer.pub
```
Add the public key to:
- GitHub repo deploy keys (read-only), and/or
- VPS `~/.ssh/authorized_keys` for a restricted reviewer user.

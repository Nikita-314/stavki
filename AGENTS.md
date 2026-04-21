# Agent runtime protocol (football live)

This repository runs a long-lived Telegram bot (`python3 -m app.main`) inside a `screen` session.

## Mandatory rule after runtime-affecting changes

After **any** code changes that can affect runtime behavior (bot startup, live loop, handlers, services,
fetch/ingest pipeline, diagnostics formatting), the agent must:

- stop the previous `screen` process
- restart the bot in a new `screen` session
- confirm the new code is executing by providing:
  - the `screen` session id/name
  - 1–2 log lines that include:
    - `Start polling`
    - `[FOOTBALL][LIVE_LOOP] started`
    - `[FOOTBALL][LIVE_LOOP] tick`

If this proof is missing, the change must be treated as **not deployed**.


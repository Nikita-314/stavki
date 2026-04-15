## Server runbook (manual via screen)

### One-time setup (project)

```bash
cd /path/to/stavki
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` and set at least:
- `BOT_TOKEN=...`
- `DATABASE_URL=...`

### Create a screen session for the bot

```bash
screen -S stavki-bot
```

### Start the bot inside screen

```bash
cd /path/to/stavki
source .venv/bin/activate
alembic upgrade head
python3 -m app.main
```

### Detach from screen (leave bot running)

- Press `Ctrl+A`, then `D`

### List screen sessions

```bash
screen -ls
```

### Re-attach to the bot session

```bash
screen -r stavki-bot
```

If it says “Attached”, use:

```bash
screen -d -r stavki-bot
```

### Stop the bot

Preferred (from inside the screen session):
- Press `Ctrl+C`

Alternative (from outside, last resort):

```bash
pkill -f "python3 -m app.main"
```

### Restart after `git pull`

Inside screen:

```bash
cd /path/to/stavki
source .venv/bin/activate
git pull
pip install -r requirements.txt
alembic upgrade head
python3 -m app.main
```

### Apply DB migrations on the server

```bash
cd /path/to/stavki
source .venv/bin/activate
alembic upgrade head
```

### Quick check that the bot responds

In Telegram (admin user), send:
- `/quick_check`
- `/system_status`
- `/debug_help`

### Remote test workflow (Telegram)

0) Check environment/config:
- `/server_checklist`
- `/odds_http_url`

1) Smoke remote ingest (fetch → preview → ingest → sanity/summary/balance):
- `/remote_smoke`

2) Settle a remote event (manual winner selection):
- `/remote_settle FOOTBALL football_30001 Зенит`
- `/remote_settle CS2 cs2_10001 Team Spirit`

3) One-button end-to-end demo (smoke + settle + status summary):
- `/remote_flow_demo FOOTBALL football_30001 Зенит`

4) After any run, check status:
- `/remote_status`
- `/sanity_check`


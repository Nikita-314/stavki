## stavki

Backend-каркас на Python для Telegram-бота сигналов по ставкам (CS2 / Dota2 / футбол).

Проект пока на этапе **каркаса**: базовая структура, конфигурация, модели БД и миграции.

### Стек

- Python 3.12
- aiogram 3
- PostgreSQL
- SQLAlchemy 2.x (async)
- Alembic (миграции)
- pydantic-settings (конфигурация)

### Локальный запуск (минимально)

1) Создать и активировать виртуальное окружение, установить зависимости:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Создать файл `.env` на основе `.env.example` и указать `BOT_TOKEN` и `DATABASE_URL`.

3) Запуск бота:

```bash
python3 -m app.main
```

### PostgreSQL и Alembic

1) Создайте базу данных PostgreSQL (пример):

```sql
CREATE DATABASE stavki;
```

2) Убедитесь, что в `.env` указан корректный `DATABASE_URL`, например:

```text
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/stavki
```

3) Применить миграции:

```bash
alembic upgrade head
```

4) Создать новую миграцию (когда появятся изменения в моделях):

```bash
alembic revision --autogenerate -m "describe change"
```

### Server manual run (screen)

См. `docs/server_runbook.md`.

Минимально (внутри screen):

```bash
source .venv/bin/activate
alembic upgrade head
python3 -m app.main
```

### Debug commands for remote testing

- **URL / preview / ingest**
  - `/odds_http_url`
  - `/odds_http_preview`
  - `/odds_http_ingest`
- **Smoke (fetch → preview → ingest → sanity/summary/balance)**
  - `/remote_smoke`
- **Settle remote event**
  - `/remote_settle <sport> <event_external_id> <winner_selection>`
- **One-button flow**
  - `/remote_flow_demo <sport> <event_external_id> <winner_selection>`
- **Post-run status**
  - `/remote_status`
  - `/sanity_check`
  - `/balance_rub`

### Automatic signal polling

Бот может сам периодически опрашивать внешний odds-style источник, сохранять новые сигналы и отправлять их в Telegram.

Основные переменные:

- `AUTO_SIGNAL_POLLING_ENABLED=true` — включает фоновый цикл
- `AUTO_SIGNAL_POLLING_INTERVAL_SECONDS=60` — интервал опроса источника
- `AUTO_SIGNAL_PREVIEW_ONLY=true` — только fetch + preview + логирование, без сохранения сигналов
- `AUTO_SIGNAL_MAX_CREATED_PER_CYCLE=5` — ограничение на число новых сигналов за один цикл

Для работы автосигналов также должны быть настроены:

- `ODDS_PROVIDER_BASE_URL`
- `ODDS_PROVIDER_SPORT`
- `ODDS_PROVIDER_MARKETS`
- при необходимости `ODDS_PROVIDER_API_KEY`
- `SIGNAL_CHAT_ID`, если вы хотите автоматически отправлять новые сигналы в Telegram

Проверка вручную:

- `/auto_signal_status` — показать текущие настройки автопотока
- `/auto_signal_run_once` — вручную выполнить один цикл fetch/preview/ingest/notify


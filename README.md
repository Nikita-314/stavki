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


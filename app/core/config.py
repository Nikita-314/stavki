from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from pydantic import BeforeValidator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_admin_user_ids(v: object) -> list[int]:
    if v is None:
        return []
    if isinstance(v, list):
        return [int(x) for x in v]
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        parts = [p.strip() for p in s.split(",")]
        return [int(p) for p in parts if p]
    return [int(v)]


class Settings(BaseSettings):
    app_name: str = "stavki-signals-bot"
    bot_token: str
    database_url: str
    debug: bool = False
    admin_user_ids: Annotated[list[int], BeforeValidator(_parse_admin_user_ids)] = []
    signal_chat_id: int | None = None
    result_chat_id: int | None = None
    virtual_flat_stake_rub: Decimal = Decimal("1000.00")
    virtual_start_balance_rub: Decimal = Decimal("50000.00")
    provider_test_url: str | None = None
    provider_test_timeout_seconds: int = 20

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def get_settings() -> Settings:
    return Settings()


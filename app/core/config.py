from __future__ import annotations

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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def get_settings() -> Settings:
    return Settings()


from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class _Settings(BaseSettings):
    BOT_TOKEN: str
    API_ID: int
    API_HASH: str
    OWNER_ID: List[int] = []

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"
    )

# noinspection PyArgumentList
settings = _Settings()
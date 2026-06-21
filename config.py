from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class _Settings(BaseSettings):
    BOT_TOKEN: str
    API_ID: int
    API_HASH: str
    OWNER_ID: List[int] = []

    CHUNK_SIZE_LIMIT: int = 2000 * 1024 * 1024

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"
    )

# noinspection PyArgumentList
settings = _Settings()
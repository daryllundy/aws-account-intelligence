from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="AAI_", extra="ignore")

    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/aws_account_intelligence",
        alias="DATABASE_URL",
    )
    data_source: str = "fixtures"
    aws_regions: str = "us-west-2,us-east-1"
    output_dir: Path = Path(".aai-output")
    idle_days: int = 30

    @property
    def region_list(self) -> list[str]:
        return [region.strip() for region in self.aws_regions.split(",") if region.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

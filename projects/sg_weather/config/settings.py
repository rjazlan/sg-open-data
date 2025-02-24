from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache

# Get root project directory
ROOT_DIR = Path(__file__).parent.parent.parent.parent
PROJECT_DIR = ROOT_DIR / "projects/sg_weather"
ENV_FILE = ROOT_DIR / ".env"


class Settings(BaseSettings):
    # API Settings
    REALTIME_API_BASE_URL: str = "https://api-open.data.gov.sg/v2/real-time/api"
    API_RATE_LIMIT: int = 30

    # Database Settings
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "weather_db"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"

    # Data Storage
    DATA_DIR: Path = PROJECT_DIR / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "raw" / "weather"
    PROCESSED_DATA_DIR: Path = DATA_DIR / "processed" / "weather"

    # Logging
    LOG_LEVEL: str = "INFO"

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="SG_WEATHER_",
    )


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Create directories if they don't exist
settings = get_settings()
settings.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
settings.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

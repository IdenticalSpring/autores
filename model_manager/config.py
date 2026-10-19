import os
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")


class ModelManagerSettings(BaseSettings):
    """Configuration settings for Model Manager application."""
    model_config = SettingsConfigDict(
        env_file=("/app/.env", ENV_PATH, ".env"),
        extra="ignore"
    )

    BASE_DIR: str = BASE_DIR
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PWD: str = "redis_password_local"
    ADMIN_PASSWORD: str = "admin123"
    SECRET_KEY: str = "autoresearching_secret_key_2026"
    TOKEN_EXPIRE_HOURS: int = 24

    # Default LLM Keys from .env
    OPEN_AI_KEY: str = ""
    GEMINI_KEY: str = ""
    GROQ_API_KEY: str = ""
    SERP_KEY: str = ""
    LOCALLLM_BASE_URL: str = "http://localhost:8000/v1"

    @property
    def clean_redis_host(self) -> str:
        return self.REDIS_HOST.strip("\"'") or "localhost"

    @property
    def clean_redis_pwd(self) -> str:
        return self.REDIS_PWD.strip("\"'")

    @property
    def clean_admin_password(self) -> str:
        return self.ADMIN_PASSWORD.strip("\"'") or "admin123"


settings = ModelManagerSettings()

from pydantic_settings import BaseSettings, SettingsConfigDict
import datetime
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

x = datetime.datetime.now()


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=("/app/.env", ENV_PATH, ".env"), extra='ignore')

    OPEN_AI_KEY: str = ""

    GEMINI_KEY: str = ""
    # DeepSeek: provider chính thay Gemini free tier
    DEEPSEEK_API_KEY: str = ""
    DEEPSEEK_BASE_URL: str = "https://api.deepseek.com/v1"
    AI_DEFAULT_MODEL: str = "deepseek-flash"
    # disabled = tắt thinking mode (mặc định). enabled = bật cho tác vụ suy luận nặng.
    DEEPSEEK_THINKING: str = "disabled"
    SERP_KEY: str = ""
    
    CURRENT_YEAR: int = x.year
    CURRENT_DATE: str = x.strftime("%B-%d-%Y")
    REDIS: str = ""
    REDIS_PWD: str = ""
    REDIS_HOST: str = ""
    REDIS_PORT: int = 0
    REDIS_TTL: int = 0
    REDIS_STACK: str = ""
    KAFKA: str = ""
    MINIO_ENDPOINT: str = ""
    MINIO_PORT: int = 0
    MINIO_ACCESS_KEY_ID: str = ""
    MINIO_SECRET_ACCESS_KEY: str = ""
    MINIO_DOMAIN: str = ""
    MINIO_BUCKET: str = "autoresearching"
    MINIO_BUCKET_ANALYSIS: str = "analysis"
    QDRANT_URL: str = ""
    EMBEDDING_MODEL: str = ""
    OPEN_ALEX_EMAIL: str = ""
    OPEN_ALEX_USER: str = ""
    OPEN_ALEX_KEY: str = ""
    TMP_FOLDER: str = ""
    MONGO_CONNECTION_STRING: str = ""
    LOCALLLM_BASE_URL: str = ""
    API_KEY_ID: str = ""
    # Webhook health-check của BE (mặc định: REDIS_HOST:4002)
    HEALTH_CHECK_HOST: str = ""
    HEALTH_CHECK_PORT: int = 4002


settings = AppSettings()
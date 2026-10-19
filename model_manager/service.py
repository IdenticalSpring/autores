import json
import logging
import os
from typing import Any, Dict, List, Optional
from redis.asyncio import Redis
from model_manager.config import settings

logger = logging.getLogger(__name__)
BACKUP_FILE = os.path.join(settings.BASE_DIR, "tmp", "models_backup.json")


def mask_key(key: str) -> str:
    """Mask sensitive API keys for safe display in UI."""
    if not key or key == "None":
        return "(Không có key)"
    if key.startswith("DUMMY_"):
        return "(Chưa cấu hình / Key giả)"
    if len(key) <= 8:
        return "********"
    return f"{key[:6]}...{key[-4:]}"


def detect_provider(model_id: str) -> str:
    """Infer the AI provider from the model ID."""
    m = model_id.lower()
    if "gemini" in m:
        return "Google Gemini"
    elif "gpt" in m or "o1" in m or "o3" in m:
        return "OpenAI"
    elif "groq" in m:
        return "Groq"
    elif "local" in m or "oss" in m:
        return "Local LLM"
    return "Custom Provider"


class RedisModelService:
    """Handles Redis storage and synchronization for AI models."""

    def __init__(self):
        self._redis: Optional[Redis] = None

    async def get_redis(self) -> Redis:
        if self._redis is None:
            self._redis = Redis(
                host=settings.clean_redis_host,
                port=settings.REDIS_PORT or 6379,
                password=settings.clean_redis_pwd or None,
                decode_responses=True
            )
        return self._redis

    @staticmethod
    def read_env_key(key_name: str) -> str:
        """Dynamically read key directly from .env file on disk."""
        candidates = ["/app/.env", os.path.join(settings.BASE_DIR, ".env"), ".env"]
        for path in candidates:
            if os.path.isfile(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        for line in f:
                            s = line.strip()
                            if s.startswith(f"{key_name}=") or s.startswith(f"export {key_name}="):
                                parts = s.split("=", 1)
                                if len(parts) == 2:
                                    val = parts[1].strip("\"' ")
                                    if val and not val.startswith("DUMMY_"):
                                        return val
                except Exception:
                    pass
        return ""

    async def get_shared_key(self, key_type: str) -> str:
        """Retrieve the active shared key for a provider from Redis or .env."""
        r = await self.get_redis()
        if key_type == "gemini":
            for candidate in ["gemini-1.5-flash", "gemini-2.0-flash", "gemini-2.5-flash", "default"]:
                raw = await r.get(f"AI_KEY:{candidate}")
                if raw:
                    try:
                        val = json.loads(raw)
                    except Exception:
                        val = raw
                    val_str = str(val).strip("\"' ")
                    if val_str and val_str != "None" and not val_str.startswith("DUMMY_"):
                        return val_str
            env_val = self.read_env_key("GEMINI_KEY")
            if env_val:
                return env_val
            if settings.GEMINI_KEY and not settings.GEMINI_KEY.startswith("DUMMY_"):
                return settings.GEMINI_KEY

        elif key_type == "openai":
            for candidate in ["gpt-4o", "gpt-4o-mini", "default"]:
                raw = await r.get(f"AI_KEY:{candidate}")
                if raw:
                    try:
                        val = json.loads(raw)
                    except Exception:
                        val = raw
                    val_str = str(val).strip("\"' ")
                    if val_str and val_str != "None" and not val_str.startswith("DUMMY_"):
                        return val_str
            env_val = self.read_env_key("OPEN_AI_KEY")
            if env_val:
                return env_val
            if settings.OPEN_AI_KEY and not settings.OPEN_AI_KEY.startswith("DUMMY_"):
                return settings.OPEN_AI_KEY

        return ""

    async def list_models(self) -> List[Dict[str, Any]]:
        """Retrieve all registered AI models from Redis."""
        r = await self.get_redis()
        keys = await r.keys("AI_KEY:*")
        models = []

        system_keys = {"AI_KEY:db_key", "AI_KEY:search_web_key", "AI_KEY:default"}

        for k in sorted(keys):
            model_id = k.replace("AI_KEY:", "")
            raw_val = await r.get(k)
            key_val = ""
            if raw_val:
                try:
                    key_val = json.loads(raw_val)
                except Exception:
                    key_val = raw_val

            is_system = k in system_keys
            is_dummy = str(key_val).startswith("DUMMY_")
            has_real_key = bool(key_val and key_val != "None" and not is_dummy)
            models.append({
                "key_name": k,
                "model_id": model_id,
                "provider": detect_provider(model_id),
                "masked_key": mask_key(str(key_val)),
                "has_key": has_real_key,
                "is_dummy": is_dummy,
                "is_system": is_system
            })
        return models

    async def set_model(self, model_id: str, key_value: str) -> bool:
        """Add or update an AI model in Redis and sync to backup."""
        r = await self.get_redis()
        key_name = f"AI_KEY:{model_id.strip()}"
        clean_key = key_value.strip("\"' ")
        await r.set(key_name, json.dumps(clean_key))
        await self.save_backup()
        logger.info(f"Updated Redis model: {key_name}")
        return True

    async def delete_model(self, model_id: str) -> bool:
        """Delete an AI model from Redis."""
        r = await self.get_redis()
        key_name = f"AI_KEY:{model_id.strip()}"
        deleted = await r.delete(key_name)
        await self.save_backup()
        return bool(deleted)

    async def get_model_key(self, model_id: str) -> str:
        """Get the actual unmasked key for internal testing."""
        r = await self.get_redis()
        raw = await r.get(f"AI_KEY:{model_id.strip()}")
        if not raw:
            return ""
        try:
            return json.loads(raw)
        except Exception:
            return raw

    async def save_backup(self) -> None:
        """Persist current Redis keys to local backup file."""
        try:
            models = await self.list_models()
            os.makedirs(os.path.dirname(BACKUP_FILE), exist_ok=True)
            with open(BACKUP_FILE, "w", encoding="utf-8") as f:
                json.dump(models, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Could not write model backup: {e}")


model_service = RedisModelService()

import time
from typing import Any, Dict
import httpx
from model_manager.config import settings
from model_manager.service import model_service


async def test_model_connection(model_id: str) -> Dict[str, Any]:
    """Test actual connectivity and response time for a model."""
    key = await model_service.get_model_key(model_id)
    if not key and model_id != "localhost":
        return {
            "success": False,
            "latency_ms": 0,
            "message": f"Chưa cấu hình API Key cho model '{model_id}'."
        }

    start_time = time.time()
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            if "gemini" in model_id.lower():
                url = (
                    f"https://generativelanguage.googleapis.com/v1beta/models/"
                    f"{model_id}:generateContent?key={key}"
                )
                payload = {
                    "contents": [{"parts": [{"text": "Hello, respond with 'OK'"}]}]
                }
                resp = await client.post(url, json=payload)
                latency = int((time.time() - start_time) * 1000)
                if resp.status_code == 200:
                    return {
                        "success": True,
                        "latency_ms": latency,
                        "message": "Kết nối thành công! Gemini phản hồi 200 OK."
                    }
                else:
                    return {
                        "success": False,
                        "latency_ms": latency,
                        "message": f"Lỗi Gemini ({resp.status_code}): {resp.text[:120]}"
                    }

            elif "gpt" in model_id.lower() or "o1" in model_id.lower():
                url = "https://api.openai.com/v1/chat/completions"
                headers = {"Authorization": f"Bearer {key}"}
                payload = {
                    "model": model_id,
                    "messages": [{"role": "user", "content": "ping"}],
                    "max_tokens": 5
                }
                resp = await client.post(url, headers=headers, json=payload)
                latency = int((time.time() - start_time) * 1000)
                if resp.status_code == 200:
                    return {
                        "success": True,
                        "latency_ms": latency,
                        "message": "Kết nối thành công! OpenAI phản hồi 200 OK."
                    }
                else:
                    return {
                        "success": False,
                        "latency_ms": latency,
                        "message": f"Lỗi OpenAI ({resp.status_code}): {resp.text[:120]}"
                    }

            else:
                base_url = settings.LOCALLLM_BASE_URL.rstrip("/")
                url = f"{base_url}/models"
                resp = await client.get(url)
                latency = int((time.time() - start_time) * 1000)
                if resp.status_code in [200, 401, 404]:
                    return {
                        "success": True,
                        "latency_ms": latency,
                        "message": f"Máy chủ cục bộ/Groq phản hồi ({resp.status_code})."
                    }
                return {
                    "success": False,
                    "latency_ms": latency,
                    "message": f"Không kết nối được: HTTP {resp.status_code}"
                }

    except Exception as e:
        latency = int((time.time() - start_time) * 1000)
        return {
            "success": False,
            "latency_ms": latency,
            "message": f"Lỗi kết nối ngoại lệ: {str(e)[:120]}"
        }

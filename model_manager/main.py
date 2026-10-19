import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from model_manager.config import settings
from model_manager.auth import generate_token, get_current_admin
from model_manager.service import model_service
from model_manager.tester import test_model_connection

app = FastAPI(
    title="AutoResearching Model Manager",
    description="Portal quản lý Model AI và API Keys trên Redis",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class LoginRequest(BaseModel):
    password: str = Field(..., description="Mật khẩu quản trị")


class ModelCreateRequest(BaseModel):
    model_id: str = Field(..., description="Tên model (vd: gemini-3.8-flash)")
    key_type: str = Field(
        "gemini",
        description="Loại key: 'gemini', 'openai', 'groq', 'custom', 'none'"
    )
    custom_key: Optional[str] = Field("", description="API key tùy chỉnh")


@app.post("/api/auth/login")
async def login(req: LoginRequest):
    """Authenticate with ADMIN_PASSWORD and issue bearer token."""
    if req.password == settings.clean_admin_password:
        token = generate_token()
        return {"status": "success", "token": token}
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Mật khẩu quản trị không chính xác."
    )


@app.get("/api/models")
async def get_models(_: str = Depends(get_current_admin)):
    """List all AI models configured in Redis."""
    models = await model_service.list_models()
    return {"status": "success", "models": models}


@app.get("/api/system/keys")
async def get_system_keys(_: str = Depends(get_current_admin)):
    """Return status of shared provider keys configured in .env or Redis."""
    has_gemini = bool(await model_service.get_shared_key("gemini"))
    has_openai = bool(await model_service.get_shared_key("openai"))
    return {
        "status": "success",
        "has_gemini": has_gemini,
        "has_openai": has_openai,
        "has_groq": bool(settings.GROQ_API_KEY),
        "has_serp": bool(settings.SERP_KEY),
    }


@app.post("/api/models")
async def add_model(req: ModelCreateRequest, _: str = Depends(get_current_admin)):
    """Add or update an AI model in Redis."""
    model_id = req.model_id.strip()
    if not model_id:
        raise HTTPException(status_code=400, detail="Tên model không được để trống")

    key_value = ""
    if req.key_type in ("gemini", "openai"):
        key_value = await model_service.get_shared_key(req.key_type)
        if not key_value:
            raise HTTPException(
                status_code=400,
                detail=f"Chưa có {req.key_type.upper()} key trong hệ thống. Vui lòng chọn 'Nhập API Key riêng' hoặc cập nhật .env."
            )
    elif req.key_type in ("groq", "none"):
        key_value = "None"
    elif req.key_type == "custom":
        key_value = req.custom_key or ""
        if not key_value:
            raise HTTPException(status_code=400, detail="Vui lòng nhập API Key")

    await model_service.set_model(model_id, key_value)
    return {
        "status": "success",
        "message": f"Đã lưu model '{model_id}' vào Redis thành công!",
        "model_id": model_id
    }


@app.delete("/api/models/{model_id}")
async def delete_model(model_id: str, _: str = Depends(get_current_admin)):
    """Delete an AI model from Redis."""
    success = await model_service.delete_model(model_id)
    if not success:
        raise HTTPException(status_code=404, detail="Model không tồn tại trong Redis")
    return {"status": "success", "message": f"Đã xóa model '{model_id}'"}


@app.post("/api/models/{model_id}/test")
async def test_model(model_id: str, _: str = Depends(get_current_admin)):
    """Run an automated ping test against the model."""
    result = await test_model_connection(model_id)
    return {"status": "success", "result": result}


# Mount static directory for Frontend Web UI
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.exists(STATIC_DIR):
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("model_manager.main:app", host="0.0.0.0", port=8088, reload=True)

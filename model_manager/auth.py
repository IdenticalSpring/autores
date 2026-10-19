import base64
import hashlib
import hmac
import time
from typing import Optional
from fastapi import HTTPException, Header, status
from model_manager.config import settings


def generate_token(username: str = "admin") -> str:
    """Create a signed bearer token with expiration timestamp."""
    exp = int(time.time()) + (settings.TOKEN_EXPIRE_HOURS * 3600)
    payload = f"{username}:{exp}"
    signature = hmac.new(
        settings.SECRET_KEY.encode(),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()
    raw = f"{payload}:{signature}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def verify_token(token: str) -> bool:
    """Verify validity and signature of the provided token."""
    try:
        raw = base64.urlsafe_b64decode(token.encode()).decode()
        username, exp_str, signature = raw.split(":", 2)
        if int(exp_str) < time.time():
            return False
        expected_sig = hmac.new(
            settings.SECRET_KEY.encode(),
            f"{username}:{exp_str}".encode(),
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(signature, expected_sig)
    except Exception:
        return False


def get_current_admin(authorization: Optional[str] = Header(None)) -> str:
    """FastAPI dependency to protect admin endpoints."""
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Thiếu Authorization Header"
        )
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Định dạng Token không hợp lệ (cần 'Bearer <token>')"
        )
    token = parts[1]
    if not verify_token(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Phiên làm việc đã hết hạn hoặc Token không hợp lệ"
        )
    return "admin"

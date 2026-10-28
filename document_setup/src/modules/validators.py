"""Validation utilities for Document Setup payloads."""
from typing import Any

VALID_LEVELS: set[str] = {
    "CHUYEN_DE",
    "LUAN_AN_TIEN_SI",
    "BAI_BAO_KHOA_HOC",
    "LUAN_VAN_THAC_SI",
    "KHOA_LUAN",
    "TIEU_LUAN",
}

VALID_LANGUAGES: set[str] = {
    "vi",
    "vietnamese",
    "tiếng việt",
    "en",
    "english",
}


def validate_level(level: Any) -> str:
    """Validate research level.

    Raises ValueError if invalid, which listener converts to Error.INVALID_PARAMS (603).
    """
    if not isinstance(level, str) or not level.strip():
        raise ValueError(f"level must be a non-empty string, got {type(level).__name__}")
    normalized = level.strip()
    if normalized not in VALID_LEVELS:
        raise ValueError(
            f"Invalid level: '{level}'. Must be one of {sorted(list(VALID_LEVELS))}"
        )
    return normalized


def validate_language(language: Any) -> str:
    """Validate payload language.

    Raises ValueError if invalid, which listener converts to Error.INVALID_PARAMS (603).
    """
    if not isinstance(language, str) or not language.strip():
        raise ValueError(f"language must be a non-empty string, got {type(language).__name__}")
    normalized = language.strip().lower()
    if normalized not in VALID_LANGUAGES:
        raise ValueError(
            f"Invalid language: '{language}'. Must be one of {sorted(list(VALID_LANGUAGES))}"
        )
    return language.strip()

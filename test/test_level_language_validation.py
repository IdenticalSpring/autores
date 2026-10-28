"""Unit test for level and language validation in document_setup."""
import pytest
from document_setup.src.modules.validators import (
    VALID_LEVELS,
    VALID_LANGUAGES,
    validate_level,
    validate_language,
)
from error_type import Error


class TestValidators:
    @pytest.mark.parametrize("level", list(VALID_LEVELS))
    def test_valid_levels_accepted(self, level: str):
        assert validate_level(level) == level
        assert validate_level(f"  {level}  ") == level

    @pytest.mark.parametrize("invalid_level", ["", "   ", "UNKNOWN", "MASTER", "PHD", 123, None])
    def test_invalid_levels_raise_value_error(self, invalid_level):
        with pytest.raises(ValueError) as excinfo:
            validate_level(invalid_level)
        assert "level" in str(excinfo.value).lower()

    @pytest.mark.parametrize("lang", ["vi", "VI", "Vietnamese", "vietnamese", "tiếng việt", "TIẾNG VIỆT", "en", "English", "english"])
    def test_valid_languages_accepted(self, lang: str):
        assert validate_language(lang) == lang.strip()

    @pytest.mark.parametrize("invalid_lang", ["", "   ", "fr", "french", "zh", "ja", 456, None])
    def test_invalid_languages_raise_value_error(self, invalid_lang):
        with pytest.raises(ValueError) as excinfo:
            validate_language(invalid_lang)
        assert "language" in str(excinfo.value).lower()


class TestListenerFailFastContract:
    def test_error_code_mapping_on_value_error(self):
        """Verify that ValueError raised by validators maps to Error.INVALID_PARAMS (603)."""
        message = {}
        try:
            validate_level("INVALID_LEVEL")
        except ValueError as e:
            message["error_code"] = Error.INVALID_PARAMS
            message["error"] = str(e)

        assert message["error_code"] == 603
        assert "Invalid level" in message["error"]


class TestLanguageDict:
    def test_get_language_dict_vietnamese_aliases(self):
        from translate import get_language_dict, LANGUAGE_KIT
        for alias in ["vi", "VI", "Vietnamese", "vietnamese", "tiếng việt", "TIẾNG VIỆT"]:
            d = get_language_dict(alias)
            assert isinstance(d, dict)
            assert d == LANGUAGE_KIT["tiếng việt"]
            assert "- what to write for subsection " in d

    def test_get_language_dict_english_aliases(self):
        from translate import get_language_dict, LANGUAGE_KIT
        for alias in ["en", "EN", "English", "english"]:
            d = get_language_dict(alias)
            assert isinstance(d, dict)
            assert d == LANGUAGE_KIT["english"]

    def test_get_language_dict_fallback_on_unknown_or_none(self):
        from translate import get_language_dict, LANGUAGE_KIT
        for unknown in [None, "", "fr", "unknown"]:
            d = get_language_dict(unknown)
            assert isinstance(d, dict)
            assert d == LANGUAGE_KIT["tiếng việt"]


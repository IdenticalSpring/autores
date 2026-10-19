import unittest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient

from model_manager.main import app
from model_manager.auth import generate_token, verify_token
from model_manager.service import mask_key, detect_provider
from model_manager.config import settings

client = TestClient(app)


class TestModelManager(unittest.TestCase):
    """Test suite for Model Manager authentication, utilities, and API endpoints."""

    def test_token_generation_and_validation(self):
        """Verify that generated tokens are valid and tamper-evident."""
        token = generate_token("admin")
        self.assertTrue(verify_token(token))
        # Tampered token should fail
        tampered = token[:-4] + "xxxx"
        self.assertFalse(verify_token(tampered))
        self.assertFalse(verify_token("invalid:token:format"))

    def test_mask_key_utility(self):
        """Ensure sensitive API keys are masked appropriately."""
        self.assertEqual(mask_key(""), "(Không có key)")
        self.assertEqual(mask_key("None"), "(Không có key)")
        self.assertEqual(mask_key("DUMMY_GEMINI_KEY"), "(Chưa cấu hình / Key giả)")
        self.assertEqual(mask_key("short"), "********")
        masked = mask_key("AIzaSyAbCdEfGhIjKlMnOpQrStUvWxYz123456")
        self.assertTrue(masked.startswith("AIzaSy..."))
        self.assertTrue(masked.endswith("3456"))
        self.assertNotIn("AbCdEf", masked)

    def test_detect_provider_utility(self):
        """Verify provider detection logic for various model names."""
        self.assertEqual(detect_provider("gemini-3.8-flash"), "Google Gemini")
        self.assertEqual(detect_provider("gemini-3.5-flash-lite"), "Google Gemini")
        self.assertEqual(detect_provider("gpt-4o-mini"), "OpenAI")
        self.assertEqual(detect_provider("gpt-5-preview"), "OpenAI")
        self.assertEqual(detect_provider("groq/mixtral"), "Groq")
        self.assertEqual(detect_provider("localhost"), "Local LLM")
        self.assertEqual(detect_provider("my-custom-llm"), "Custom Provider")

    def test_login_api(self):
        """Test login endpoint with correct and incorrect admin password."""
        # Incorrect password
        res_fail = client.post("/api/auth/login", json={"password": "wrong_password_999"})
        self.assertEqual(res_fail.status_code, 401)

        # Correct password
        res_ok = client.post("/api/auth/login", json={"password": settings.clean_admin_password})
        self.assertEqual(res_ok.status_code, 200)
        data = res_ok.json()
        self.assertEqual(data["status"], "success")
        self.assertIn("token", data)
        self.assertTrue(verify_token(data["token"]))

    def test_unauthorized_access(self):
        """Ensure endpoints reject requests without a valid Bearer token."""
        res = client.get("/api/models")
        self.assertEqual(res.status_code, 401)

        res_bad_header = client.get("/api/models", headers={"Authorization": "Basic 123456"})
        self.assertEqual(res_bad_header.status_code, 401)

    @patch("model_manager.service.model_service.list_models", new_callable=AsyncMock)
    def test_get_models_authorized(self, mock_list):
        """Test retrieving models list when authenticated."""
        mock_list.return_value = [
            {
                "model_id": "gemini-3.8-flash",
                "provider": "Google Gemini",
                "masked_key": "AIzaSy...1234",
                "has_key": True,
                "is_system": False
            }
        ]
        token = generate_token()
        res = client.get("/api/models", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body["status"], "success")
        self.assertEqual(len(body["models"]), 1)
        self.assertEqual(body["models"][0]["model_id"], "gemini-3.8-flash")

    @patch("model_manager.service.model_service.get_shared_key", new_callable=AsyncMock)
    @patch("model_manager.service.model_service.set_model", new_callable=AsyncMock)
    def test_add_model_authorized(self, mock_set, mock_get_shared):
        """Test adding a new model via the API."""
        mock_get_shared.return_value = "AIzaSyTestSharedKey12345"
        mock_set.return_value = True
        token = generate_token()
        payload = {
            "model_id": "gemini-3.8-flash",
            "key_type": "gemini",
            "custom_key": ""
        }
        res = client.post("/api/models", json=payload, headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["status"], "success")
        mock_set.assert_called_once()

    @patch("model_manager.service.model_service.delete_model", new_callable=AsyncMock)
    def test_delete_model_authorized(self, mock_del):
        """Test deleting a model via the API."""
        mock_del.return_value = True
        token = generate_token()
        res = client.delete("/api/models/gemini-3.8-flash", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["status"], "success")


if __name__ == "__main__":
    unittest.main()


import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import main


class TestAuthAccountModes(unittest.TestCase):
    def setUp(self):
        self.client = main.app.test_client()
        self._original_auth_enforce = main.AUTH_ENFORCE
        self._original_auth_db_path = main.AUTH_DB_PATH
        self._temp_dir = tempfile.TemporaryDirectory()
        main.AUTH_DB_PATH = Path(self._temp_dir.name) / "auth_state.sqlite3"
        main.AUTH_ENFORCE = True
        main._ensure_auth_tables()

    def tearDown(self):
        main.AUTH_ENFORCE = self._original_auth_enforce
        main.AUTH_DB_PATH = self._original_auth_db_path
        self._temp_dir.cleanup()

    def _create_verified_user(self, email):
        user, error = main._create_auth_user(
            email,
            "StrongPass123!",
            "Test",
            "User",
        )
        self.assertIsNone(error or None)
        self.assertIsNotNone(user)
        main._mark_auth_user_verified(int(user["id"]))
        return user

    def test_auth_templates_render(self):
        self.assertEqual(self.client.get("/auth/login").status_code, 200)
        self.assertEqual(self.client.get("/auth/register").status_code, 200)
        self.assertEqual(self.client.get("/auth/forgot-password").status_code, 200)

    def test_non_admin_catalog_hides_fixed_career_sleeves(self):
        user = self._create_verified_user("someone@example.com")
        with self.client.session_transaction() as session_state:
            session_state["auth_user_id"] = int(user["id"])

        response = self.client.get("/synergy-sleeves")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        self.assertEqual(payload.get("fixed"), [])
        self.assertEqual(payload.get("custom_letter_min"), "A")
        self.assertFalse(payload.get("show_scope_labels"))

    def test_admin_catalog_keeps_fixed_career_sleeves(self):
        admin = self._create_verified_user("thijs.vanzon@existenceinitiative.com")
        with self.client.session_transaction() as session_state:
            session_state["auth_user_id"] = int(admin["id"])

        response = self.client.get("/synergy-sleeves")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        fixed_letters = [entry.get("letter") for entry in (payload.get("fixed") or [])]
        self.assertEqual(fixed_letters, ["A", "B", "C", "D"])
        self.assertEqual(payload.get("custom_letter_min"), "E")
        self.assertTrue(payload.get("show_scope_labels"))

    def test_non_admin_scrape_uses_generic_scoring_profile(self):
        user = self._create_verified_user("user2@example.com")
        with self.client.session_transaction() as session_state:
            session_state["auth_user_id"] = int(user["id"])

        fake_funnel = {
            "raw": 0,
            "after_dedupe": 0,
            "pass_count": 0,
            "maybe_count": 0,
            "fail_count": 0,
            "full_description_count": 0,
            "full_description_coverage": 0.0,
            "top_fail_reasons": [],
        }
        fake_ranking = {
            "jobs": [],
            "funnel": fake_funnel,
            "top_fail_reasons": [],
            "fallbacks_applied": [],
        }
        with patch("main.fetch_jobs_from_sources", return_value=([], [], ["indeed_web"], main._new_diagnostics())):
            with patch("main.rank_and_filter_jobs", return_value=fake_ranking):
                response = self.client.get(
                    "/scrape?career_sleeve=A&search_queries=operations+analyst"
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        summary = payload.get("summary") or {}
        self.assertEqual(summary.get("career_sleeve"), "A")
        self.assertEqual(summary.get("scoring_profile_career_sleeve"), "E")
        self.assertTrue(summary.get("custom_mode"))

    def test_register_requires_first_and_last_name(self):
        response = self.client.post(
            "/auth/register",
            data={
                "first_name": "",
                "last_name": "",
                "email": "newperson@example.com",
                "password": "StrongPass123!",
                "password_confirm": "StrongPass123!",
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("First name is required", body)

    def test_register_persists_name_and_notifies_signup_email(self):
        with patch("main._send_verification_email", return_value=True):
            with patch("main._send_signup_notification", return_value=True) as mocked_notify:
                response = self.client.post(
                    "/auth/register",
                    data={
                        "first_name": "Ada",
                        "last_name": "Lovelace",
                        "email": "ada@example.com",
                        "password": "StrongPass123!",
                        "password_confirm": "StrongPass123!",
                    },
                )
        self.assertEqual(response.status_code, 200)
        created = main._auth_user_by_email("ada@example.com")
        self.assertIsNotNone(created)
        self.assertEqual(created.get("first_name"), "Ada")
        self.assertEqual(created.get("last_name"), "Lovelace")
        mocked_notify.assert_called_once()

    def test_admin_customer_list_is_available(self):
        admin = self._create_verified_user("thijs.vanzon@existenceinitiative.com")
        self._create_verified_user("person@example.com")
        with self.client.session_transaction() as session_state:
            session_state["auth_user_id"] = int(admin["id"])
        response = self.client.get("/auth/customers")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Customer list", body)
        self.assertIn("person@example.com", body)


if __name__ == "__main__":
    unittest.main()

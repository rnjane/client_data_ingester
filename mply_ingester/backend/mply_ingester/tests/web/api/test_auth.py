import logging
import unittest
import bcrypt

from fastapi.testclient import TestClient

from mply_ingester.web.app import make_app
from mply_ingester.tests.test_utils.base import DBTestCase


logger = logging.getLogger(__name__)


class AuthApiTestCase(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        app = make_app(cls.config_broker)
        cls.client = TestClient(app)
        email = "testuser@example.com"
        password = "testpass123"
        cls.signup_data = {
            "full_name": "Test User",
            "email": email,
            "password": password,
            "company_name": "TestCo",
            "company_address": "123 Test St",
        }
        cls.login_data = {
            "username": email,
            "password": password,
        }

class SignupTestCase(AuthApiTestCase):
    def test_signup_success(self):
        resp = self.client.post("/auth/signup", data=self.signup_data)
        data = resp.json()
        print(data)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(data["email"], self.signup_data["email"])
        self.assertEqual(data["full_name"], self.signup_data["full_name"])
        self.assertEqual(data["company_name"], self.signup_data["company_name"])

class SignupDuplicateEmailTestCase(AuthApiTestCase):
    def test_signup_duplicate_email(self):
        # First sign-up
        self.client.post("/auth/signup", data=self.signup_data)
        # Second sign-up with the same email
        resp = self.client.post("/auth/signup", data=self.signup_data)
        print(resp.json())
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Email already registered", resp.text)

class LoginLogoutTestCase(AuthApiTestCase):
    def test_login_success(self):
        # Ensure user exists
        resp = self.client.post("/auth/signup", data=self.signup_data)
        self.assertEqual(resp.status_code, 200)
        resp = self.client.post("/auth/login", data=self.login_data)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["email"], self.signup_data["email"])
        self.assertEqual(data["full_name"], self.signup_data["full_name"])
        # Check session cookie
        self.assertIn("session_token", resp.cookies)

    def test_login_wrong_password(self):
        self.client.post("/auth/signup", data=self.signup_data)
        resp = self.client.post("/auth/login", data=self.login_data | {"password": "wrongPassword"},
        )
        self.assertEqual(resp.status_code, 401)
        self.assertIn("Invalid email or password", resp.text)

    def test_logout(self):
        # Sign up and login
        signup_resp = self.client.post("/auth/signup", data=self.signup_data)
        login_resp = self.client.post("/auth/login", data=self.login_data)
        cookies = self.client.cookies
        # Logout
        resp = self.client.post("/auth/logout")
        logger.debug(resp.json())
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Successfully logged out", resp.text)

if __name__ == "__main__":
    unittest.main()

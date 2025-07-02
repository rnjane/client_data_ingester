import unittest
import bcrypt

from fastapi.testclient import TestClient

from mply_ingester.web.app import app
from mply_ingester.tests.test_utils.base import DBTestCase

class AuthApiTestCase(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = TestClient(app)
        cls.signup_data = {
            "company_name": "TestCo",
            "address": "123 Test St",
            "user_name": "Test User",
            "email": "testuser@example.com",
            "password": "testpass123"
        }

class SignupTestCase(AuthApiTestCase):
    def test_signup_success(self):
        resp = self.client.post("/auth/signup", json=self.signup_data)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["email"], self.signup_data["email"])
        self.assertEqual(data["name"], self.signup_data["user_name"])
        self.assertEqual(data["company_name"], self.signup_data["company_name"])

    def test_signup_duplicate_email(self):
        # First sign-up
        self.client.post("/auth/signup", json=self.signup_data)
        # Second sign-up with same email
        resp = self.client.post("/auth/signup", json=self.signup_data)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Email already registered", resp.text)

class LoginLogoutTestCase(AuthApiTestCase):
    def test_login_success(self):
        # Ensure user exists
        self.client.post("/auth/signup", json=self.signup_data)
        resp = self.client.post(
            "/auth/login",
            auth=(self.signup_data["email"], self.signup_data["password"])
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["email"], self.signup_data["email"])
        self.assertEqual(data["name"], self.signup_data["user_name"])
        # Check session cookie
        self.assertIn("session_token", resp.cookies)

    def test_login_wrong_password(self):
        self.client.post("/auth/signup", json=self.signup_data)
        resp = self.client.post(
            "/auth/login",
            auth=(self.signup_data["email"], "wrongpassword")
        )
        self.assertEqual(resp.status_code, 401)
        self.assertIn("Invalid email or password", resp.text)

    def test_logout(self):
        # Sign up and login
        self.client.post("/auth/signup", json=self.signup_data)
        login_resp = self.client.post(
            "/auth/login",
            auth=(self.signup_data["email"], self.signup_data["password"])
        )
        cookies = login_resp.cookies
        # Logout
        resp = self.client.post("/auth/logout", cookies=cookies)
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Successfully logged out", resp.text)

if __name__ == "__main__":
    unittest.main()

import unittest
import io
import csv
import json
from fastapi.testclient import TestClient
from mply_ingester.web.app import make_app
from mply_ingester.tests.test_utils.base import DBTestCase
from mply_ingester.db.models import ClientProduct, User
from sqlalchemy import select, text

class BaseProductApiTestCase(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        app = make_app(cls.config_broker)
        cls.client1 = TestClient(app)
        cls.client2 = TestClient(app)
        # User 1
        cls.signup_data_1 = {
            "full_name": "Test User 1",
            "email": "testuser1@example.com",
            "password": "testpass123",
            "company_name": "TestCo1",
            "company_address": "123 Test St",
        }
        cls.login_data_1 = {
            "username": cls.signup_data_1["email"],
            "password": cls.signup_data_1["password"],
        }
        # User 2
        cls.signup_data_2 = {
            "full_name": "Test User 2",
            "email": "testuser2@example.com",
            "password": "testpass456",
            "company_name": "TestCo2",
            "company_address": "456 Test Ave",
        }
        cls.login_data_2 = {
            "username": cls.signup_data_2["email"],
            "password": cls.signup_data_2["password"],
        }
        # Sign up and login both users with their respective clients
        cls.client1.post("/auth/signup", data=cls.signup_data_1)
        resp1 = cls.client1.post("/auth/login", data=cls.login_data_1)
        assert resp1.status_code == 200
        user1 = cls.session.scalar(
            select(User).where(User.email == cls.signup_data_1["email"])
        )
        cls.client_id_1 = user1.client_id

        cls.client2.post("/auth/signup", data=cls.signup_data_2)
        resp2 = cls.client2.post("/auth/login", data=cls.login_data_2)
        assert resp2.status_code == 200
        user2 = cls.session.scalar(
            select(User).where(User.email == cls.signup_data_2["email"])
        )
        cls.client_id_2 = user2.client_id

    def setUp(self):
        super().setUp()
        self.session.execute(text("TRUNCATE TABLE client_products"))
        self.session.commit()

    def create_product(self, client_id, **kwargs):
        prod = ClientProduct(client_id=client_id, **kwargs)
        self.session.add(prod)
        self.session.commit()
        return prod

class ProductListApiTestCase(BaseProductApiTestCase):
    def list_products(self, client, **params):
        resp = client.get("/products/list", params=params)
        return resp

    def test_list_no_products(self):
        resp = self.list_products(self.client1)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), [])

    def test_list_few_products(self):
        # Add products for user 1
        p1 = self.create_product(self.client_id_1, sku="SKU1", title="Product 1", active=True)
        p2 = self.create_product(self.client_id_1, sku="SKU2", title="Product 2", active=True)
        # Add product for user 2
        self.create_product(self.client_id_2, sku="SKU3", title="Other User Product", active=True)
        resp = self.list_products(self.client1)
        print(resp.json())
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(data), 2)
        skus = {p["sku"] for p in data}
        self.assertIn("SKU1", skus)
        self.assertIn("SKU2", skus)
        self.assertNotIn("SKU3", skus)

    def test_list_pagination(self):
        # Insert 7 products for user 1
        for i in range(7):
            self.create_product(self.client_id_1, sku=f"SKU{i}", title=f"Product {i}", active=True)
        # Insert 2 products for user 2
        for i in range(2):
            self.create_product(self.client_id_2, sku=f"U2SKU{i}", title=f"U2 Product {i}", active=True)
        # Default limit is 5
        resp = self.list_products(self.client1)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.json()), 5)
        
        # Offset 5, limit 2
        resp = self.list_products(self.client1, s=5, l=10)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["sku"], "SKU5")
        self.assertEqual(data[1]["sku"], "SKU6")
        # User 2 should only see their own products
        resp2 = self.list_products(self.client2)
        data2 = resp2.json()
        skus2 = {p["sku"] for p in data2}
        self.assertTrue(all(sku.startswith("U2SKU") for sku in skus2))

class ProductIngestApiTestCase(BaseProductApiTestCase):
    def generate_csv_file(self, num_rows):
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["sku", "title", "active"])
        writer.writeheader()
        for i in range(num_rows):
            writer.writerow({
                "sku": f"SKU{i}",
                "title": f"Product {i}",
                "active": "1"
            })
        return output.getvalue().encode("utf-8")

    def ingest_products(self, client, file_bytes, parser_config=None):
        if parser_config is None:
            parser_config = {
                "parser_id": "csv",
                "column_mapping": {
                    "sku": ["sku", "text"],
                    "title": ["title", "text"],
                    "active": ["active", "boolean"]
                }
            }
        files = {"data_file": ("products.csv", file_bytes, "text/csv")}
        resp = client.post(
            "/products/ingest",
            data={'parser_config': json.dumps(parser_config)},
            files=files,
        )
        return resp

    def test_ingest_small_file(self):
        file_bytes = self.generate_csv_file(3)
        resp = self.ingest_products(self.client1, file_bytes)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["success"])
        self.assertEqual(data["processed_items"], 3)
        # Check DB for user 1
        products = self.session.query(ClientProduct).filter_by(client_id=self.client_id_1).all()
        self.assertEqual(len(products), 3)
        # Add a product for user 2 and check isolation
        self.create_product(self.client_id_2, sku="U2SKU1", title="U2 Product 1", active=True)
        products2 = self.session.query(ClientProduct).filter_by(client_id=self.client_id_2).all()
        self.assertEqual(len(products2), 1)

    def test_ingest_large_file(self):
        file_bytes = self.generate_csv_file(50)
        resp = self.ingest_products(self.client1, file_bytes)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        print(data)
        self.assertTrue(data["success"])
        self.assertEqual(data["processed_items"], 50)
        products = self.session.query(ClientProduct).filter_by(client_id=self.client_id_1).all()
        self.assertEqual(len(products), 50)
        # Add a product for user 2 and check isolation
        self.create_product(self.client_id_2, sku="U2SKU2", title="U2 Product 2", active=True)
        products2 = self.session.query(ClientProduct).filter_by(client_id=self.client_id_2).all()
        self.assertEqual(len(products2), 1)

if __name__ == "__main__":
    unittest.main()

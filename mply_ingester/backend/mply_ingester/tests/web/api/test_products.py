import unittest
import io
import csv
import json
from fastapi.testclient import TestClient
from mply_ingester.web.app import make_app
from mply_ingester.tests.test_utils.base import DBTestCase
from mply_ingester.db.models import ClientProduct, User
import pytest
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
        # Refresh session to ensure we see committed changes
        self.refresh_session()
        
        return resp

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
    def generate_csv_file(self, num_rows, active=True):
        assert isinstance(active, bool)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["sku", "title", "active"])
        writer.writeheader()
        for i in range(num_rows):
            writer.writerow({
                "sku": f"SKU{i}",
                "title": f"Product {i}",
                "active": "1" if active else "0"
            })
        return output.getvalue().encode("utf-8")

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

    @pytest.mark.xfail(reason="The assignment requires you to cause this to pass")
    def test_ingest_updates_active_status(self):
        # First ingestion: all products active
        file_bytes_active = self.generate_csv_file(3)
        resp1 = self.ingest_products(self.client1, file_bytes_active)
        self.assertEqual(resp1.status_code, 200)
        data1 = resp1.json()
        self.assertTrue(data1["success"])
        self.assertEqual(data1["processed_items"], 3)
        # Check all products are active
        products = self.session.query(ClientProduct).filter_by(client_id=self.client_id_1).all()
        self.assertEqual(len(products), 3)
        self.assertTrue(all(p.active for p in products))

        # Second ingestion: same SKUs, but all inactive
        # For any client_product, the sku is the unique identifier,
        # Therefore we expect that this next bit will change the active status of the existing products
        # And not create new ones
        file_bytes_inactive = self.generate_csv_file(3, active=False)
        resp2 = self.ingest_products(self.client1, file_bytes_inactive)
        self.assertEqual(resp2.status_code, 200)
        data2 = resp2.json()
        self.assertTrue(data2["success"])
        self.assertEqual(data2["processed_items"], 3)
        # Check all products are now inactive
        products_after = self.session.query(ClientProduct).filter_by(client_id=self.client_id_1).all()
        self.assertEqual(len(products_after), 3)
        self.assertTrue(all(not p.active for p in products_after))

    def test_ingest_records_without_sku(self):
        """Test that all records are processed, including those with empty SKUs."""
        csv_data = [
            {"sku": "SKU1", "title": "Product 1", "active": "1"},
            {"sku": "", "title": "Product 2", "active": "1"},  # Empty SKU
            {"sku": "", "title": "Product 3", "active": "0"},  # Empty SKU
        ]
        file_bytes = self._create_csv_file(csv_data)
        
        resp = self.ingest_products(self.client1, file_bytes)
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["success"])
        self.assertEqual(resp.json()["processed_items"], 3)
        
        # All records should be created
        products = self.session.query(ClientProduct).filter_by(client_id=self.client_id_1).all()
        self.assertEqual(len(products), 3)
        
        # Verify products with empty SKUs were created
        empty_sku_products = [p for p in products if p.sku == ""]
        self.assertEqual(len(empty_sku_products), 2)

    def test_ingest_mixed_sku_scenarios(self):
        """Test mixed scenarios: existing SKU, new SKU, and empty SKU."""
        # Create existing product
        self.create_product(self.client_id_1, sku="EXISTING", title="Old Title", active=True)
        
        csv_data = [
            {"sku": "EXISTING", "title": "Updated Title", "active": "0"},  # Update existing
            {"sku": "NEW_SKU", "title": "New Product", "active": "1"},     # New SKU
            {"sku": "", "title": "No SKU Product", "active": "1"},         # Empty SKU (creates new)
        ]
        file_bytes = self._create_csv_file(csv_data)
        
        resp = self.ingest_products(self.client1, file_bytes)
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["success"])
        self.assertEqual(resp.json()["processed_items"], 3)
        
        # Should have 3 products: one updated, two new
        products = self.session.query(ClientProduct).filter_by(client_id=self.client_id_1).all()
        self.assertEqual(len(products), 3)
        
        # Verify existing product was updated
        existing = self.session.query(ClientProduct).filter_by(sku="EXISTING").first()
        self.assertEqual(existing.title, "Updated Title")
        self.assertFalse(existing.active)
        
        # Verify new product was created
        new_product = self.session.query(ClientProduct).filter_by(sku="NEW_SKU").first()
        self.assertIsNotNone(new_product)
        self.assertEqual(new_product.title, "New Product")

    def test_ingest_sku_not_in_database(self):
        """Test that new SKUs create new records."""
        file_bytes = self.generate_csv_file(2)
        resp = self.ingest_products(self.client1, file_bytes)
        
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["success"])
        self.assertEqual(resp.json()["processed_items"], 2)
        
        # Verify products were created
        products = self.session.query(ClientProduct).filter_by(client_id=self.client_id_1).all()
        self.assertEqual(len(products), 2)
        self.assertIn("SKU0", [p.sku for p in products])
        self.assertIn("SKU1", [p.sku for p in products])

    def _create_csv_file(self, data):
        """Helper method to create CSV file from data."""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["sku", "title", "active"])
        writer.writeheader()
        for row in data:
            writer.writerow(row)
        return output.getvalue().encode("utf-8")

class ProductFullUpdateApiTestCase(BaseProductApiTestCase):
    def generate_csv_file(self, rows):
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["sku", "title", "active"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return output.getvalue().encode("utf-8")

    def ingest_products_full_update(self, client, file_bytes, parser_config=None):
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
            data={'parser_config': json.dumps(parser_config), 'full_update': True},
            files=files
        )
        self.refresh_session()
        return resp

    def test_full_update_deactivates_absent_products(self):
        """Full update: products not in file are deactivated."""
        self.create_product(self.client_id_1, sku="A", title="Product A", active=True)
        self.create_product(self.client_id_1, sku="B", title="Product B", active=True)
        # Ingest only A
        csv_data = [
            {"sku": "A", "title": "Product A Updated", "active": "1"}
        ]
        file_bytes = self.generate_csv_file(csv_data)
        resp = self.ingest_products_full_update(self.client1, file_bytes)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["success"])
        # A is active and updated, B is deactivated
        a = self.session.query(ClientProduct).filter_by(sku="A", client_id=self.client_id_1).first()
        b = self.session.query(ClientProduct).filter_by(sku="B", client_id=self.client_id_1).first()
        self.assertTrue(a.active)
        self.assertEqual(a.title, "Product A Updated")
        self.assertFalse(b.active)

    def test_default_mode_does_not_deactivate(self):
        """Default mode: products not in file remain active."""
        self.create_product(self.client_id_1, sku="A", title="Product A", active=True)
        self.create_product(self.client_id_1, sku="B", title="Product B", active=True)
        # Ingest only A
        csv_data = [
            {"sku": "A", "title": "Product A Updated", "active": "1"}
        ]
        file_bytes = self.generate_csv_file(csv_data)
        resp = self.ingest_products(self.client1, file_bytes)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["success"])
        # A is active and updated, B is still active
        a = self.session.query(ClientProduct).filter_by(sku="A", client_id=self.client_id_1).first()
        b = self.session.query(ClientProduct).filter_by(sku="B", client_id=self.client_id_1).first()
        self.assertTrue(a.active)
        self.assertEqual(a.title, "Product A Updated")
        self.assertTrue(b.active)

if __name__ == "__main__":
    unittest.main()

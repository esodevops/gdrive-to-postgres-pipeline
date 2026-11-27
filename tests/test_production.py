import os
import unittest
from decimal import Decimal
from pathlib import Path

from pandas.errors import DatabaseError
from sqlalchemy import create_engine, text

from production import publish_snapshot, validate_snapshot


CUSTOMERS = b"""Customer ID,Full Name,Email Address,Phone Number,City,Join Date,Customer Segment
C1,Alice Example,ALICE@example.com,,Helsinki,2024-01-01,Retail
"""
ACCOUNTS = b"""Account ID,Customer ID,Account Type,Currency,Opening Balance,Open Date,Account Status
A1,C1,Current,EUR,100.00,2024-01-01,Active
"""
TRANSACTIONS = b"""Transaction ID,Account ID,Transaction Date,Transaction Type,Description,Category,Amount,Currency,Channel,Status
T1,A1,2024-01-02,Purchase,Groceries,Food,-10.15,EUR,Card,Completed
"""


def snapshot(amount=b"-10.15"):
    return {
        "customers": validate_snapshot("customers", CUSTOMERS, "customers.csv"),
        "accounts": validate_snapshot("accounts", ACCOUNTS, "accounts.csv"),
        "transactions": validate_snapshot(
            "transactions", TRANSACTIONS.replace(b"-10.15", amount), "transactions.csv"
        ),
    }


class ValidationTest(unittest.TestCase):
    def test_preserves_exact_money_and_normalizes_outflow_sign(self):
        for value in (b"10.15", b"-10.15"):
            with self.subTest(value=value):
                row = snapshot(value)["transactions"].iloc[0]
                self.assertEqual(row["signed_amount"], Decimal("-10.15"))

    def test_rejects_missing_columns_and_invalid_records(self):
        for payload in (
            TRANSACTIONS.replace(b",Amount", b",Unexpected"),
            TRANSACTIONS.rstrip() + b",extra\n",
            TRANSACTIONS.replace(b"-10.15", b"invalid"),
            TRANSACTIONS.replace(b"2024-01-02", b"invalid"),
            TRANSACTIONS + TRANSACTIONS.splitlines(keepends=True)[1],
        ):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                validate_snapshot("transactions", payload, "transactions.csv")


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "requires a disposable PostgreSQL database")
class PublishTest(unittest.TestCase):
    def test_atomic_publish_and_duplicate_delivery(self):
        engine = create_engine(os.environ["TEST_DATABASE_URL"])
        self.addCleanup(engine.dispose)
        migration = Path(__file__).resolve().parents[1] / "sql" / "001_production.sql"
        with engine.connect() as conn:
            conn.exec_driver_sql(migration.read_text())
            conn.commit()

        publish_snapshot(engine, "first", snapshot(), {})
        publish_snapshot(engine, "first", snapshot(b"20.00"), {})
        broken_snapshot = snapshot(b"30.00")
        broken_snapshot["transactions"] = broken_snapshot["transactions"].assign(account_id="missing")
        with self.assertRaises(DatabaseError):
            publish_snapshot(engine, "broken", broken_snapshot, {})

        with engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("SELECT signed_amount FROM reporting.transactions")).scalar_one(),
                Decimal("-10.15"),
            )
            self.assertEqual(
                conn.execute(text("SELECT batch_id FROM reporting.refresh_status")).scalar_one(),
                "first",
            )
            self.assertEqual(conn.execute(text("SELECT COUNT(*) FROM ingestion.published_batches")).scalar_one(), 1)

        publish_snapshot(engine, "second", snapshot(b"20.00"), {})
        with engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("SELECT signed_amount FROM reporting.transactions")).scalar_one(),
                Decimal("-20.00"),
            )

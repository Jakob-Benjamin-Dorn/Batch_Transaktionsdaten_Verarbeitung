from __future__ import annotations

import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col

from src.spark.data_quality import mark_duplicate_transactions
from src.spark.schemas import TRANSACTION_BRONZE_SCHEMA
from src.spark.transform_transactions import (
    prepare_transactions,
    split_by_quality,
)


class TestSparkTransformation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("test-spark-transformation")
            .master("local[1]")
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_bronze_is_split_into_silver_and_quarantine(self):
        rows = [
            # Gültiger Purchase
            (
                "txn_valid",
                "cus_1",
                "mer_1",
                "dev_1",
                "2026-01-01T10:00:00",
                "2026-01-01T10:05:00",
                25.0,
                "eur",
                "DE",
                "Berlin",
                "card",
                "purchase",
                "approved",
                False,
                False,
                False,
                "MOBILE_APP ",
                None,
            ),
            # Ungültig: customer_id fehlt und Currency ist ungültig
            (
                "txn_invalid",
                None,
                "mer_2",
                "dev_2",
                "2026-01-01T11:00:00",
                "2026-01-01T11:05:00",
                30.0,
                "XXX",
                "DE",
                "Hamburg",
                "paypal",
                "purchase",
                "approved",
                False,
                False,
                False,
                "web_checkout",
                None,
            ),
            # Älteres Duplikat
            (
                "txn_duplicate",
                "cus_3",
                "mer_3",
                "dev_3",
                "2026-01-01T12:00:00",
                "2026-01-01T12:05:00",
                40.0,
                "EUR",
                "DE",
                "München",
                "card",
                "purchase",
                "approved",
                False,
                False,
                False,
                "payment_gateway_v1",
                None,
            ),
            # Neueres Duplikat – diese Zeile darf in Silver bleiben
            (
                "txn_duplicate",
                "cus_3",
                "mer_3",
                "dev_3",
                "2026-01-01T12:00:00",
                "2026-01-01T12:30:00",
                40.0,
                "EUR",
                "DE",
                "München",
                "card",
                "purchase",
                "approved",
                False,
                False,
                False,
                "payment_gateway_v1",
                None,
            ),
        ]

        bronze = self.spark.createDataFrame(
            rows,
            schema=TRANSACTION_BRONZE_SCHEMA,
        )

        prepared = prepare_transactions(
            transactions=bronze,
            logical_date="2026-01-01",
        )

        prepared = mark_duplicate_transactions(prepared)

        silver, quarantine = split_by_quality(prepared)

        input_rows = bronze.count()
        valid_rows = silver.count()
        quarantined_rows = quarantine.count()

        # Keine Zeile darf verschwinden.
        self.assertEqual(
            input_rows,
            valid_rows + quarantined_rows,
        )

        # Erwartung:
        # txn_valid + neuestes txn_duplicate → Silver
        # txn_invalid + älteres txn_duplicate → Quarantine
        self.assertEqual(valid_rows, 2)
        self.assertEqual(quarantined_rows, 2)

        # Silver enthält keine DQ-Hilfsspalten.
        self.assertNotIn("dq_errors", silver.columns)
        self.assertNotIn("is_valid", silver.columns)
        self.assertNotIn("duplicate_rank", silver.columns)

        # Silver enthält echte, normalisierte Felder.
        valid_row = silver.filter(col("transaction_id") == "txn_valid").first()

        self.assertEqual(valid_row["currency"], "EUR")
        self.assertEqual(
            valid_row["source_system"],
            "mobile_app",
        )
        self.assertIsNotNone(valid_row["event_time"])

        # Silver darf keine doppelten transaction_id enthalten.
        duplicate_ids = (
            silver.groupBy("transaction_id").count().filter(col("count") > 1).count()
        )

        self.assertEqual(duplicate_ids, 0)

        # Die fachlich ungültige Zeile besitzt beide erwarteten Fehler.
        invalid_row = quarantine.filter(col("transaction_id") == "txn_invalid").first()

        self.assertIn(
            "DQ001_REQUIRED_CUSTOMER_ID",
            invalid_row["dq_errors"],
        )
        self.assertIn(
            "DQ002_INVALID_CURRENCY",
            invalid_row["dq_errors"],
        )

        # Das ältere Duplikat wird explizit markiert.
        duplicate_row = quarantine.filter(
            array_contains(
                col("dq_errors"),
                "DQ007_DUPLICATE_TRANSACTION",
            )
        ).first()

        self.assertIsNotNone(duplicate_row)
        self.assertEqual(
            duplicate_row["duplicate_rank"],
            2,
        )

    def test_invalid_ingestion_time_goes_to_quarantine(self):
        rows = [
            (
                "txn_invalid_ingestion_time",
                "cus_1",
                "mer_1",
                "dev_1",
                "2026-01-01T10:00:00",
                "not-a-timestamp",
                25.0,
                "EUR",
                "DE",
                "Berlin",
                "card",
                "purchase",
                "approved",
                False,
                False,
                False,
                "mobile_app",
                None,
            )
        ]

        bronze = self.spark.createDataFrame(
            rows,
            schema=TRANSACTION_BRONZE_SCHEMA,
        )

        prepared = prepare_transactions(
            transactions=bronze,
            logical_date="2026-01-01",
        )

        prepared = mark_duplicate_transactions(prepared)

        silver, quarantine = split_by_quality(prepared)

        self.assertEqual(
            silver.count(),
            0,
        )

        self.assertEqual(
            quarantine.count(),
            1,
        )

        quarantined_row = quarantine.first()

        self.assertIn(
            "DQ010_INVALID_INGESTION_TIME",
            quarantined_row["dq_errors"],
        )

        self.assertFalse(
            quarantined_row["is_valid"],
        )


if __name__ == "__main__":
    unittest.main()

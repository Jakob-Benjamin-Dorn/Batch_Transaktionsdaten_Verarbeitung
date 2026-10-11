from __future__ import annotations

import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.spark.data_quality import (
    invalid_ingestion_time,
    mark_duplicate_transactions,
    missing_transaction_id,
)
from src.spark.schemas import TRANSACTION_BRONZE_SCHEMA
from src.spark.transform_transactions import prepare_transactions


class TestDataQualityRules(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("test-data-quality")
            .master("local[1]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def prepare(self, rows):
        dataframe = self.spark.createDataFrame(
            rows,
            schema=TRANSACTION_BRONZE_SCHEMA,
        )

        prepared = prepare_transactions(
            dataframe,
            "2026-01-01",
        )

        return mark_duplicate_transactions(prepared)

    def test_valid_purchase_has_no_errors(self):
        dataframe = self.prepare(
            [
                (
                    "txn_1",
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
                )
            ]
        )

        row = dataframe.first()

        self.assertTrue(row["is_valid"])
        self.assertEqual(row["dq_errors"], [])
        self.assertEqual(
            row["currency_normalized"],
            "EUR",
        )
        self.assertEqual(
            row["source_system_normalized"],
            "mobile_app",
        )

    def test_invalid_values_are_collected(self):
        dataframe = self.prepare(
            [
                (
                    "txn_2",
                    None,
                    "mer_1",
                    "dev_1",
                    "not-a-timestamp",
                    "2026-01-01T10:05:00",
                    -20.0,
                    "XXX",
                    "DE",
                    "Berlin",
                    "card",
                    "purchase",
                    "approved",
                    False,
                    False,
                    False,
                    "unknown_system",
                    None,
                )
            ]
        )

        errors = set(dataframe.first()["dq_errors"])

        self.assertIn(
            "DQ001_REQUIRED_CUSTOMER_ID",
            errors,
        )
        self.assertIn(
            "DQ002_INVALID_CURRENCY",
            errors,
        )
        self.assertIn(
            "DQ003_INVALID_EVENT_TIME",
            errors,
        )
        self.assertIn(
            "DQ005_INVALID_PURCHASE_AMOUNT",
            errors,
        )
        self.assertIn(
            "DQ008_INVALID_SOURCE_SYSTEM",
            errors,
        )

    def test_invalid_refund_is_detected(self):
        dataframe = self.prepare(
            [
                (
                    "txn_3",
                    "cus_1",
                    "mer_1",
                    "dev_1",
                    "2026-01-01T10:00:00",
                    "2026-01-01T10:05:00",
                    20.0,
                    "EUR",
                    "DE",
                    "Berlin",
                    "card",
                    "refund",
                    "approved",
                    False,
                    False,
                    False,
                    "mobile_app",
                    None,
                )
            ]
        )

        errors = dataframe.first()["dq_errors"]

        self.assertIn(
            "DQ006_INVALID_REFUND",
            errors,
        )

    def test_older_duplicate_is_rejected(self):
        dataframe = self.prepare(
            [
                (
                    "txn_duplicate",
                    "cus_1",
                    "mer_1",
                    "dev_1",
                    "2026-01-01T10:00:00",
                    "2026-01-01T10:05:00",
                    20.0,
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
                ),
                (
                    "txn_duplicate",
                    "cus_1",
                    "mer_1",
                    "dev_1",
                    "2026-01-01T10:00:00",
                    "2026-01-01T11:00:00",
                    20.0,
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
                ),
            ]
        )

        rows = dataframe.orderBy("ingestion_time_parsed").collect()

        self.assertIn(
            "DQ007_DUPLICATE_TRANSACTION",
            rows[0]["dq_errors"],
        )

        self.assertNotIn(
            "DQ007_DUPLICATE_TRANSACTION",
            rows[1]["dq_errors"],
        )

    def test_missing_transaction_id_detects_null(self):
        schema = StructType(
            [
                StructField(
                    "transaction_id",
                    StringType(),
                    True,
                ),
            ]
        )

        dataframe = self.spark.createDataFrame(
            [
                {
                    "transaction_id": None,
                }
            ],
            schema=schema,
        )

        result = dataframe.select(
            missing_transaction_id().alias("is_invalid")
        ).collect()[0]["is_invalid"]

        self.assertTrue(result)

    def test_missing_transaction_id_detects_empty_string(self):
        schema = StructType(
            [
                StructField(
                    "transaction_id",
                    StringType(),
                    True,
                ),
            ]
        )

        dataframe = self.spark.createDataFrame(
            [
                {
                    "transaction_id": "   ",
                }
            ],
            schema=schema,
        )

        result = dataframe.select(
            missing_transaction_id().alias("is_invalid")
        ).collect()[0]["is_invalid"]

        self.assertTrue(result)

    def test_invalid_ingestion_time_detects_null_raw_value(self):
        schema = StructType(
            [
                StructField(
                    "ingestion_time",
                    StringType(),
                    True,
                ),
                StructField(
                    "ingestion_time_parsed",
                    StringType(),
                    True,
                ),
            ]
        )

        dataframe = self.spark.createDataFrame(
            [
                {
                    "ingestion_time": None,
                    "ingestion_time_parsed": None,
                }
            ],
            schema=schema,
        )

        result = dataframe.select(
            invalid_ingestion_time().alias("is_invalid")
        ).collect()[0]["is_invalid"]

        self.assertTrue(result)

    def test_invalid_ingestion_time_detects_parse_failure(self):
        schema = StructType(
            [
                StructField(
                    "ingestion_time",
                    StringType(),
                    True,
                ),
                StructField(
                    "ingestion_time_parsed",
                    StringType(),
                    True,
                ),
            ]
        )

        dataframe = self.spark.createDataFrame(
            [
                {
                    "ingestion_time": "not-a-timestamp",
                    "ingestion_time_parsed": None,
                }
            ],
            schema=schema,
        )

        result = dataframe.select(
            invalid_ingestion_time().alias("is_invalid")
        ).collect()[0]["is_invalid"]

        self.assertTrue(result)

    def test_missing_transaction_id_is_not_marked_as_duplicate(self):
        dataframe = self.prepare(
            [
                (
                    None,
                    "cus_1",
                    "mer_1",
                    "dev_1",
                    "2026-01-01T10:00:00",
                    "2026-01-01T10:05:00",
                    20.0,
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
                ),
                (
                    None,
                    "cus_2",
                    "mer_2",
                    "dev_2",
                    "2026-01-01T10:00:00",
                    "2026-01-01T10:06:00",
                    30.0,
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
                ),
            ]
        )

        rows = dataframe.collect()

        for row in rows:
            self.assertIn(
                "DQ009_REQUIRED_TRANSACTION_ID",
                row["dq_errors"],
            )
            self.assertNotIn(
                "DQ007_DUPLICATE_TRANSACTION",
                row["dq_errors"],
            )


if __name__ == "__main__":
    unittest.main()

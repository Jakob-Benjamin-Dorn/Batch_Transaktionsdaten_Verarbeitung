from __future__ import annotations

import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.spark.build_gold import (
    build_country_currency_summary,
    build_daily_kpis,
)


SILVER_TEST_SCHEMA = StructType(
    [
        StructField(
            "transaction_id",
            StringType(),
            False,
        ),
        StructField(
            "event_time",
            TimestampType(),
            True,
        ),
        StructField(
            "amount",
            DoubleType(),
            False,
        ),
        StructField(
            "currency",
            StringType(),
            False,
        ),
        StructField(
            "transaction_country",
            StringType(),
            False,
        ),
        StructField(
            "transaction_type",
            StringType(),
            False,
        ),
        StructField(
            "status",
            StringType(),
            False,
        ),
        StructField(
            "is_fraud",
            BooleanType(),
            False,
        ),
        StructField(
            "is_late_arriving",
            BooleanType(),
            False,
        ),
    ]
)


class TestGoldAggregations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .appName("test-gold-aggregations")
            .master("local[1]")
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_transaction(
        self,
        transaction_id: str,
        amount: float,
        transaction_type: str = "purchase",
        status: str = "approved",
        currency: str = "EUR",
        country: str = "DE",
        is_fraud: bool = False,
        is_late_arriving: bool = False,
    ) -> dict:
        return {
            "transaction_id": transaction_id,
            "event_time": None,
            "amount": amount,
            "currency": currency,
            "transaction_country": country,
            "transaction_type": transaction_type,
            "status": status,
            "is_fraud": is_fraud,
            "is_late_arriving": is_late_arriving,
        }

    def test_daily_kpis_are_calculated_correctly(self):
        rows = [
            self.make_transaction(
                transaction_id="txn_1",
                amount=100.0,
            ),
            self.make_transaction(
                transaction_id="txn_2",
                amount=50.0,
                is_fraud=True,
            ),
            self.make_transaction(
                transaction_id="txn_3",
                amount=-20.0,
                transaction_type="refund",
            ),
            self.make_transaction(
                transaction_id="txn_4",
                amount=40.0,
                status="declined",
                is_late_arriving=True,
            ),
        ]

        silver = self.spark.createDataFrame(
            rows,
            schema=SILVER_TEST_SCHEMA,
        )

        result = build_daily_kpis(
            silver=silver,
            logical_date="2026-01-01",
        ).first()

        self.assertEqual(
            result["total_transactions"],
            4,
        )
        self.assertEqual(
            result["approved_transactions"],
            3,
        )
        self.assertEqual(
            result["declined_transactions"],
            1,
        )
        self.assertEqual(
            result["purchase_transactions"],
            3,
        )
        self.assertEqual(
            result["refund_transactions"],
            1,
        )
        self.assertEqual(
            result["fraud_transactions"],
            1,
        )
        self.assertEqual(
            result["late_arriving_transactions"],
            1,
        )
        self.assertEqual(
            result["total_purchase_amount"],
            150.0,
        )
        self.assertEqual(
            result["total_refund_amount"],
            -20.0,
        )
        self.assertEqual(
            result["net_amount"],
            130.0,
        )
        self.assertEqual(
            result["approval_rate"],
            0.75,
        )
        self.assertEqual(
            result["fraud_rate"],
            0.25,
        )
        self.assertEqual(
            result["refund_rate"],
            0.25,
        )

    def test_country_currency_summary_is_grouped(self):
        rows = [
            self.make_transaction(
                transaction_id="txn_1",
                amount=100.0,
                country="DE",
                currency="EUR",
            ),
            self.make_transaction(
                transaction_id="txn_2",
                amount=50.0,
                country="DE",
                currency="EUR",
                is_fraud=True,
            ),
            self.make_transaction(
                transaction_id="txn_3",
                amount=80.0,
                country="US",
                currency="USD",
            ),
        ]

        silver = self.spark.createDataFrame(
            rows,
            schema=SILVER_TEST_SCHEMA,
        )

        result = build_country_currency_summary(
            silver=silver,
            logical_date="2026-01-01",
        ).collect()

        by_key = {
            (
                row["transaction_country"],
                row["currency"],
            ): row
            for row in result
        }

        germany = by_key[("DE", "EUR")]
        united_states = by_key[("US", "USD")]

        self.assertEqual(
            germany["transaction_count"],
            2,
        )
        self.assertEqual(
            germany["fraud_count"],
            1,
        )
        self.assertEqual(
            germany["net_amount"],
            150.0,
        )

        self.assertEqual(
            united_states["transaction_count"],
            1,
        )
        self.assertEqual(
            united_states["net_amount"],
            80.0,
        )


if __name__ == "__main__":
    unittest.main()

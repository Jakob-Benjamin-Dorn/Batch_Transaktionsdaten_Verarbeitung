from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

TRANSACTION_BRONZE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("device_id", StringType(), True),
        # Raw timestamps are intentionally read as strings.
        # Invalid source values such as "not-a-timestamp" must remain visible
        # until the data-quality checks are applied.
        StructField("event_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_country", StringType(), True),
        StructField("transaction_city", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("is_refund", BooleanType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("is_late_arriving", BooleanType(), True),
        StructField("source_system", StringType(), True),
        # This field is optional and appears only after the simulated
        # schema-drift date. Spark fills it with null when it is absent.
        StructField("device_trust_score", DoubleType(), True),
    ]
)

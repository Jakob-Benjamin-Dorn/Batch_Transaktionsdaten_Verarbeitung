from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    array_compact,
    array_contains,
    col,
    lit,
    lower,
    size,
    trim,
    try_to_timestamp,
    upper,
    when,
)

from src.spark.data_quality import (
    event_time_in_future,
    invalid_currency,
    invalid_event_time,
    invalid_ingestion_time,
    invalid_purchase_amount,
    invalid_refund,
    invalid_source_system,
    mark_duplicate_transactions,
    missing_customer_id,
    missing_transaction_id,
)
from src.spark.schemas import TRANSACTION_BRONZE_SCHEMA


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate one daily Bronze transaction batch and write "
            "Silver, Quarantine and a quality report."
        )
    )

    parser.add_argument(
        "--logical-date",
        required=True,
        help="Batch date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--data-dir",
        default="/opt/airflow/data",
        help="Base data directory.",
    )

    return parser.parse_args()


def validate_logical_date(logical_date: str) -> None:
    try:
        datetime.strptime(logical_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            "--logical-date must use the format YYYY-MM-DD"
        ) from exc


def partition_path(
    data_dir: str,
    layer: str,
    logical_date: str,
) -> str:
    batch_date = datetime.strptime(logical_date, "%Y-%m-%d")

    return str(
        Path(data_dir)
        / layer
        / "transactions"
        / f"year={batch_date.year:04d}"
        / f"month={batch_date.month:02d}"
        / f"day={batch_date.day:02d}"
    )


def read_bronze(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    return (
        spark.read
        .schema(TRANSACTION_BRONZE_SCHEMA)
        .json(input_path)
    )


def prepare_transactions(
    transactions: DataFrame,
    logical_date: str,
) -> DataFrame:
    prepared = (
        transactions
        .withColumn(
            "customer_id",
            trim(col("customer_id")),
        )
        .withColumn(
            "transaction_id",
            trim(col("transaction_id")),
        )
        .withColumn(
            "currency_normalized",
            upper(trim(col("currency"))),
        )
        .withColumn(
            "source_system_normalized",
            lower(trim(col("source_system"))),
        )
        .withColumn(
            "event_time_parsed",
            try_to_timestamp(col("event_time")),
        )
        .withColumn(
            "ingestion_time_parsed",
            try_to_timestamp(col("ingestion_time")),
        )
    )

    return (
        prepared
        .withColumn(
            "dq_errors",
            array_compact(
                array(
                    when(
                        missing_customer_id(),
                        lit("DQ001_REQUIRED_CUSTOMER_ID"),
                    ),
                    when(
                        invalid_currency(),
                        lit("DQ002_INVALID_CURRENCY"),
                    ),
                    when(
                        invalid_event_time(),
                        lit("DQ003_INVALID_EVENT_TIME"),
                    ),
                    when(
                        event_time_in_future(logical_date),
                        lit("DQ004_EVENT_TIME_IN_FUTURE"),
                    ),
                    when(
                        invalid_purchase_amount(),
                        lit("DQ005_INVALID_PURCHASE_AMOUNT"),
                    ),
                    when(
                        invalid_refund(),
                        lit("DQ006_INVALID_REFUND"),
                    ),
                    when(
                        invalid_source_system(),
                        lit("DQ008_INVALID_SOURCE_SYSTEM"),
                    ),
                    when(
                        missing_transaction_id(),
                        lit("DQ009_REQUIRED_TRANSACTION_ID"),
                    ),
                    when(
                        invalid_ingestion_time(),
                        lit("DQ010_INVALID_INGESTION_TIME"),
                    ),
                )
            ),
        )
        .withColumn(
            "is_valid",
            size(col("dq_errors")) == 0,
        )
    )


def split_by_quality(
    prepared: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    silver = (
        prepared
        .filter(col("is_valid"))
        .drop(
            "event_time",
            "ingestion_time",
            "currency",
            "source_system",
            "dq_errors",
            "is_valid",
            "duplicate_rank",
        )
        .withColumnRenamed(
            "event_time_parsed",
            "event_time",
        )
        .withColumnRenamed(
            "ingestion_time_parsed",
            "ingestion_time",
        )
        .withColumnRenamed(
            "currency_normalized",
            "currency",
        )
        .withColumnRenamed(
            "source_system_normalized",
            "source_system",
        )
    )

    quarantine = prepared.filter(~col("is_valid"))

    return silver, quarantine


def create_quality_report(
    input_rows: int,
    valid_rows: int,
    quarantined_rows: int,
    logical_date: str,
    quarantine: DataFrame,
) -> dict:
    rule_counts = {}

    rule_ids = [
        "DQ001_REQUIRED_CUSTOMER_ID",
        "DQ002_INVALID_CURRENCY",
        "DQ003_INVALID_EVENT_TIME",
        "DQ004_EVENT_TIME_IN_FUTURE",
        "DQ005_INVALID_PURCHASE_AMOUNT",
        "DQ006_INVALID_REFUND",
        "DQ007_DUPLICATE_TRANSACTION",
        "DQ008_INVALID_SOURCE_SYSTEM",
        "DQ009_REQUIRED_TRANSACTION_ID",
        "DQ010_INVALID_INGESTION_TIME",
    ]

    for rule_id in rule_ids:
        rule_counts[rule_id] = quarantine.filter(
            array_contains(
                col("dq_errors"),
                rule_id,
            )
        ).count()

    validity_rate = (
        valid_rows / input_rows
        if input_rows > 0
        else 0.0
    )

    return {
        "logical_date": logical_date,
        "status": (
            "passed"
            if quarantined_rows == 0
            else "warning"
        ),
        "input_rows": input_rows,
        "valid_rows": valid_rows,
        "quarantined_rows": quarantined_rows,
        "validity_rate": round(validity_rate, 4),
        "rule_failures": rule_counts,
    }


def write_quality_report(
    report: dict,
    data_dir: str,
    logical_date: str,
) -> Path:
    batch_date = datetime.strptime(logical_date, "%Y-%m-%d")

    output_path = (
        Path(data_dir)
        / "reports"
        / f"year={batch_date.year:04d}"
        / f"month={batch_date.month:02d}"
        / f"day={batch_date.day:02d}"
        / "quality_report.json"
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            report,
            file,
            indent=2,
            ensure_ascii=False,
        )

    return output_path


def main() -> None:
    args = parse_args()
    validate_logical_date(args.logical_date)

    bronze_path = (
        partition_path(
            data_dir=args.data_dir,
            layer="bronze_raw",
            logical_date=args.logical_date,
        )
        + "/transactions.jsonl"
    )

    silver_path = partition_path(
        data_dir=args.data_dir,
        layer="silver_processed",
        logical_date=args.logical_date,
    )

    quarantine_path = partition_path(
        data_dir=args.data_dir,
        layer="quarantine",
        logical_date=args.logical_date,
    )

    if not Path(bronze_path).is_file():
        raise FileNotFoundError(
            f"Bronze batch does not exist: {bronze_path}"
        )

    spark = (
        SparkSession.builder
        .appName(
            f"transform-transactions-{args.logical_date}"
        )
        .master("local[2]")
        .getOrCreate()
    )

    try:
        bronze = read_bronze(
            spark=spark,
            input_path=bronze_path,
        )

        prepared = prepare_transactions(
            transactions=bronze,
            logical_date=args.logical_date,
        )

        prepared = mark_duplicate_transactions(prepared)

        silver, quarantine = split_by_quality(prepared)

        input_rows = bronze.count()
        valid_rows = silver.count()
        quarantined_rows = quarantine.count()

        (
            silver.write
            .mode("overwrite")
            .parquet(silver_path)
        )

        (
            quarantine.write
            .mode("overwrite")
            .parquet(quarantine_path)
        )

        report = create_quality_report(
            input_rows=input_rows,
            valid_rows=valid_rows,
            quarantined_rows=quarantined_rows,
            logical_date=args.logical_date,
            quarantine=quarantine,
        )

        report_path = write_quality_report(
            report=report,
            data_dir=args.data_dir,
            logical_date=args.logical_date,
        )

        print()
        print("Spark transaction processing completed.")
        print(f"Logical date:      {args.logical_date}")
        print(f"Input rows:        {input_rows:,}")
        print(f"Valid rows:        {valid_rows:,}")
        print(f"Quarantined rows:  {quarantined_rows:,}")
        print(f"Silver path:       {silver_path}")
        print(f"Quarantine path:   {quarantine_path}")
        print(f"Quality report:    {report_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

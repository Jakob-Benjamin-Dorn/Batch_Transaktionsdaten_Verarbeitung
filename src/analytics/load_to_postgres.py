from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from src.common.dates import validate_logical_date

DAILY_KPI_COLUMNS = (
    "logical_date",
    "total_transactions",
    "approved_transactions",
    "declined_transactions",
    "pending_transactions",
    "purchase_transactions",
    "refund_transactions",
    "fraud_transactions",
    "late_arriving_transactions",
    "total_purchase_amount",
    "total_refund_amount",
    "net_amount",
    "average_approved_purchase_amount",
    "fraud_rate",
    "refund_rate",
    "approval_rate",
)

COUNTRY_SUMMARY_COLUMNS = (
    "logical_date",
    "transaction_country",
    "currency",
    "transaction_count",
    "approved_count",
    "fraud_count",
    "net_amount",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load the Gold business metrics of one batch day "
            "into the analytics PostgreSQL database."
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

    parser.add_argument(
        "--sql-dir",
        default="/opt/airflow/sql/init",
        help="Directory with idempotent DDL scripts (*.sql).",
    )

    return parser.parse_args()


def metrics_path(data_dir: str, logical_date: str) -> Path:
    batch_date = datetime.strptime(logical_date, "%Y-%m-%d")

    return (
        Path(data_dir)
        / "reports"
        / f"year={batch_date.year:04d}"
        / f"month={batch_date.month:02d}"
        / f"day={batch_date.day:02d}"
        / "business_metrics.json"
    )


def read_metrics(path: Path, logical_date: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Business metrics do not exist: {path}")

    with path.open(encoding="utf-8") as file:
        metrics = json.load(file)

    if metrics.get("logical_date") != logical_date:
        raise ValueError(
            "logical_date in business_metrics.json "
            f"({metrics.get('logical_date')!r}) does not match "
            f"the requested batch ({logical_date!r})."
        )

    if not metrics.get("daily_kpis"):
        raise ValueError("business_metrics.json contains no daily_kpis.")

    return metrics


def pick(
    row: dict[str, Any],
    columns: tuple[str, ...],
) -> dict[str, Any]:
    missing = [name for name in columns if name not in row]

    if missing:
        raise KeyError(f"Missing columns in business metrics: {missing}")

    return {name: row[name] for name in columns}


def insert_statement(
    table: str,
    columns: tuple[str, ...],
) -> str:
    column_list = ", ".join(columns)
    placeholders = ", ".join(f"%({name})s" for name in columns)

    return f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"


def apply_ddl(connection, sql_dir: Path) -> int:
    scripts = sorted(sql_dir.glob("*.sql"))

    if not scripts:
        raise FileNotFoundError(f"No SQL scripts found in {sql_dir}")

    with connection.cursor() as cursor:
        for script in scripts:
            cursor.execute(script.read_text(encoding="utf-8"))

    connection.commit()

    return len(scripts)


def load_gold(
    connection,
    metrics: dict[str, Any],
    logical_date: str,
) -> tuple[int, int]:
    """Replace the rows of one batch day in a single transaction.

    DELETE + INSERT per logical_date makes re-runs and backfills idempotent.
    On any error nothing is changed.
    """
    daily = pick(metrics["daily_kpis"], DAILY_KPI_COLUMNS)

    countries = [
        pick(row, COUNTRY_SUMMARY_COLUMNS)
        for row in metrics.get("country_currency_summary", [])
    ]

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM analytics.daily_kpis WHERE logical_date = %s",
                (logical_date,),
            )
            cursor.execute(
                insert_statement(
                    "analytics.daily_kpis",
                    DAILY_KPI_COLUMNS,
                ),
                daily,
            )

            cursor.execute(
                "DELETE FROM analytics.country_currency_summary "
                "WHERE logical_date = %s",
                (logical_date,),
            )
            if countries:
                cursor.executemany(
                    insert_statement(
                        "analytics.country_currency_summary",
                        COUNTRY_SUMMARY_COLUMNS,
                    ),
                    countries,
                )

        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return 1, len(countries)


def connect():
    # Import hier, damit Tests und Modul-Import ohne Treiber funktionieren.
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("ANALYTICS_DB_HOST", "analytics-db"),
        port=int(os.environ.get("ANALYTICS_DB_PORT", "5432")),
        dbname=os.environ.get("ANALYTICS_DB_NAME", "analytics"),
        user=os.environ.get("ANALYTICS_DB_USER", "analytics"),
        password=os.environ["ANALYTICS_DB_PASSWORD"],
        connect_timeout=10,
    )


def main() -> None:
    args = parse_args()
    validate_logical_date(args.logical_date)

    path = metrics_path(args.data_dir, args.logical_date)
    metrics = read_metrics(path, args.logical_date)

    connection = connect()

    try:
        script_count = apply_ddl(connection, Path(args.sql_dir))
        daily_rows, country_rows = load_gold(
            connection,
            metrics,
            args.logical_date,
        )
    finally:
        connection.close()

    print()
    print("Analytics load completed.")
    print(f"Logical date:         {args.logical_date}")
    print(f"DDL scripts applied:  {script_count}")
    print(f"daily_kpis rows:      {daily_rows}")
    print(f"country summary rows: {country_rows}")


if __name__ == "__main__":
    main()

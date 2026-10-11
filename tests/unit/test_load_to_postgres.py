from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.analytics.load_to_postgres import (
    COUNTRY_SUMMARY_COLUMNS,
    DAILY_KPI_COLUMNS,
    apply_ddl,
    insert_statement,
    load_gold,
    metrics_path,
    pick,
    read_metrics,
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise RuntimeError("simulated database error")
        self.connection.calls.append(("execute", sql, params))

    def executemany(self, sql, rows):
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise RuntimeError("simulated database error")
        self.connection.calls.append(("executemany", sql, list(rows)))


class FakeConnection:
    def __init__(self, fail_on: str | None = None):
        self.calls = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_on = fail_on

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def sample_metrics(logical_date: str = "2026-01-01") -> dict:
    daily = {name: 1 for name in DAILY_KPI_COLUMNS}
    daily["logical_date"] = logical_date
    daily["average_approved_purchase_amount"] = None

    countries = [
        {
            "logical_date": logical_date,
            "transaction_country": country,
            "currency": currency,
            "transaction_count": 10,
            "approved_count": 8,
            "fraud_count": 1,
            "net_amount": 123.45,
        }
        for country, currency in [("DE", "EUR"), ("PL", "PLN")]
    ]

    return {
        "logical_date": logical_date,
        "daily_kpis": daily,
        "country_currency_summary": countries,
    }


class TestLoadToPostgres(unittest.TestCase):
    def test_metrics_path_uses_report_partitioning(self):
        path = metrics_path("/data", "2026-01-02")

        self.assertEqual(
            str(path),
            "/data/reports/year=2026/month=01/day=02/"
            "business_metrics.json",
        )

    def test_insert_statement_uses_named_placeholders(self):
        sql = insert_statement("analytics.t", ("a", "b"))

        self.assertEqual(
            sql,
            "INSERT INTO analytics.t (a, b) VALUES (%(a)s, %(b)s)",
        )

    def test_pick_reports_missing_columns(self):
        with self.assertRaises(KeyError) as context:
            pick({"a": 1}, ("a", "b"))

        self.assertIn("b", str(context.exception))

    def test_read_metrics_rejects_wrong_date(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "business_metrics.json"
            path.write_text(
                json.dumps(sample_metrics("2026-01-01")),
                encoding="utf-8",
            )

            self.assertEqual(
                read_metrics(path, "2026-01-01")["logical_date"],
                "2026-01-01",
            )

            with self.assertRaises(ValueError):
                read_metrics(path, "2026-01-02")

    def test_read_metrics_requires_existing_file(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaises(FileNotFoundError),
        ):
            read_metrics(
                Path(directory) / "missing.json",
                "2026-01-01",
            )

    def test_load_gold_replaces_the_day_in_one_transaction(self):
        connection = FakeConnection()

        daily_rows, country_rows = load_gold(
            connection,
            sample_metrics(),
            "2026-01-01",
        )

        self.assertEqual((daily_rows, country_rows), (1, 2))
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)

        statements = [call[1] for call in connection.calls]

        self.assertTrue(statements[0].startswith("DELETE FROM analytics.daily_kpis"))
        self.assertTrue(statements[1].startswith("INSERT INTO analytics.daily_kpis"))
        self.assertTrue(
            statements[2].startswith(
                "DELETE FROM analytics.country_currency_summary"
            )
        )
        self.assertTrue(
            statements[3].startswith(
                "INSERT INTO analytics.country_currency_summary"
            )
        )

        country_call = connection.calls[3]
        self.assertEqual(country_call[0], "executemany")
        self.assertEqual(len(country_call[2]), 2)
        self.assertEqual(
            tuple(country_call[2][0]),
            COUNTRY_SUMMARY_COLUMNS,
        )

    def test_load_gold_handles_empty_country_summary(self):
        metrics = sample_metrics()
        metrics["country_currency_summary"] = []
        connection = FakeConnection()

        self.assertEqual(
            load_gold(connection, metrics, "2026-01-01"),
            (1, 0),
        )
        self.assertFalse(
            any(call[0] == "executemany" for call in connection.calls)
        )

    def test_load_gold_rolls_back_on_error(self):
        connection = FakeConnection(
            fail_on="INSERT INTO analytics.country_currency_summary"
        )

        with self.assertRaises(RuntimeError):
            load_gold(connection, sample_metrics(), "2026-01-01")

        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)

    def test_load_gold_fails_before_touching_db_on_missing_column(self):
        metrics = sample_metrics()
        del metrics["daily_kpis"]["fraud_rate"]
        connection = FakeConnection()

        with self.assertRaises(KeyError):
            load_gold(connection, metrics, "2026-01-01")

        self.assertEqual(connection.calls, [])

    def test_apply_ddl_runs_scripts_in_order(self):
        with tempfile.TemporaryDirectory() as directory:
            sql_dir = Path(directory)
            (sql_dir / "002_b.sql").write_text("SELECT 2;")
            (sql_dir / "001_a.sql").write_text("SELECT 1;")
            connection = FakeConnection()

            count = apply_ddl(connection, sql_dir)

            self.assertEqual(count, 2)
            self.assertEqual(
                [call[1] for call in connection.calls],
                ["SELECT 1;", "SELECT 2;"],
            )
            self.assertEqual(connection.commits, 1)

    def test_apply_ddl_requires_scripts(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaises(FileNotFoundError),
        ):
            apply_ddl(FakeConnection(), Path(directory))


if __name__ == "__main__":
    unittest.main()

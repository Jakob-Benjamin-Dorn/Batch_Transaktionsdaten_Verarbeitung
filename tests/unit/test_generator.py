from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.generator.generate_transactions import (
    Config,
    generate_customers,
    generate_devices,
    generate_merchants,
    generate_transactions,
    inject_data_quality_issues,
    save_bronze_batch,
    seed_from_date,
    set_seed,
)


class TestTransactionGenerator(unittest.TestCase):
    def make_small_batch(self, logical_date: str):
        cfg = Config(
            start_date=logical_date,
            n_days=1,
            n_customers=20,
            n_merchants=5,
            avg_transactions_per_day=30,
        )

        set_seed(seed_from_date(logical_date, cfg.seed))

        customers = generate_customers(cfg)
        merchants = generate_merchants(cfg)
        devices = generate_devices(customers)

        clean = generate_transactions(
            cfg=cfg,
            customers=customers,
            merchants=merchants,
            devices=devices,
        )

        dirty = inject_data_quality_issues(cfg, clean)

        return cfg, dirty

    def test_seed_is_stable_for_same_date(self):
        self.assertEqual(
            seed_from_date("2026-01-01"),
            seed_from_date("2026-01-01"),
        )

    def test_different_dates_have_different_seeds(self):
        self.assertNotEqual(
            seed_from_date("2026-01-01"),
            seed_from_date("2026-01-02"),
        )

    def test_same_date_generates_reproducible_rows(self):
        _, first = self.make_small_batch("2026-01-01")
        _, second = self.make_small_batch("2026-01-01")

        self.assertEqual(
            first.to_json(orient="records", lines=True, date_format="iso"),
            second.to_json(orient="records", lines=True, date_format="iso"),
        )

    def test_schema_drift_starts_on_configured_date(self):
        _, before = self.make_small_batch("2026-01-19")
        _, after = self.make_small_batch("2026-01-20")

        self.assertNotIn("device_trust_score", before.columns)
        self.assertIn("device_trust_score", after.columns)

    def test_bronze_file_is_partitioned_and_valid_jsonl(self):
        cfg, dirty = self.make_small_batch("2026-01-01")

        with tempfile.TemporaryDirectory() as temporary_directory:
            cfg.output_dir = temporary_directory

            output_path = save_bronze_batch(
                cfg=cfg,
                transactions_dirty=dirty,
            )

            expected_path = (
                Path(temporary_directory)
                / "bronze_raw"
                / "transactions"
                / "year=2026"
                / "month=01"
                / "day=01"
                / "transactions.jsonl"
            )

            self.assertEqual(output_path, expected_path)
            self.assertTrue(output_path.is_file())
            self.assertGreater(output_path.stat().st_size, 0)

            with output_path.open(encoding="utf-8") as file:
                rows = [json.loads(line) for line in file]

            self.assertGreater(len(rows), 0)


if __name__ == "__main__":
    unittest.main()

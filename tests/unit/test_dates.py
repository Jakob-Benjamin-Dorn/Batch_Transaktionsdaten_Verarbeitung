from __future__ import annotations

import unittest

from src.common.dates import validate_logical_date


class TestValidateLogicalDate(unittest.TestCase):
    def test_accepts_valid_dates(self):
        for value in ("2026-01-01", "2024-02-29", "2026-12-31"):
            with self.subTest(value=value):
                validate_logical_date(value)

    def test_rejects_invalid_dates(self):
        invalid_values = (
            "",
            "01.01.2026",
            "2026/01/01",
            "20260101",
            "2026-13-01",
            "2026-02-30",
            "2025-02-29",
            "not-a-date",
        )

        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as context:
                    validate_logical_date(value)

                self.assertIn("YYYY-MM-DD", str(context.exception))


if __name__ == "__main__":
    unittest.main()

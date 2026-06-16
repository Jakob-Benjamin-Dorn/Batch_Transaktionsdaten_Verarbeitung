from __future__ import annotations

import unittest

from src.spark.evaluate_quality_gate import (
    evaluate_quality_gate,
    normalize_report_rule_id,
)


class TestQualityGate(unittest.TestCase):
    def setUp(self):
        self.contract = {
            "quality_rules": [
                {
                    "rule_id": "DQ001",
                    "severity": "high",
                },
                {
                    "rule_id": "DQ002",
                    "severity": "medium",
                },
            ],
            "quality_gate": {
                "minimum_validity_rate": 0.85,
                "maximum_high_severity_failure_rate": 0.1,
            },
        }

    def test_report_rule_id_is_normalized(self):
        self.assertEqual(
            normalize_report_rule_id(
                "DQ001_REQUIRED_CUSTOMER_ID"
            ),
            "DQ001",
        )

    def test_valid_report_passes(self):
        report = {
            "input_rows": 100,
            "validity_rate": 0.9,
            "rule_failures": {
                "DQ001_REQUIRED_CUSTOMER_ID": 5,
                "DQ002_INVALID_CURRENCY": 20,
            },
        }

        failures = evaluate_quality_gate(
            quality_report=report,
            contract=self.contract,
        )

        self.assertEqual(
            failures,
            [],
        )

    def test_low_validity_rate_fails(self):
        report = {
            "input_rows": 100,
            "validity_rate": 0.8,
            "rule_failures": {},
        }

        failures = evaluate_quality_gate(
            quality_report=report,
            contract=self.contract,
        )

        self.assertTrue(failures)
        self.assertIn(
            "Validity rate",
            failures[0],
        )

    def test_high_severity_failure_rate_fails(self):
        report = {
            "input_rows": 100,
            "validity_rate": 0.9,
            "rule_failures": {
                "DQ001_REQUIRED_CUSTOMER_ID": 11,
            },
        }

        failures = evaluate_quality_gate(
            quality_report=report,
            contract=self.contract,
        )

        self.assertEqual(
            len(failures),
            1,
        )

        self.assertIn(
            "DQ001_REQUIRED_CUSTOMER_ID",
            failures[0],
        )

    def test_medium_rule_does_not_trigger_high_gate(self):
        report = {
            "input_rows": 100,
            "validity_rate": 0.9,
            "rule_failures": {
                "DQ002_INVALID_CURRENCY": 50,
            },
        }

        failures = evaluate_quality_gate(
            quality_report=report,
            contract=self.contract,
        )

        self.assertEqual(
            failures,
            [],
        )

    def test_empty_batch_fails(self):
        report = {
            "input_rows": 0,
            "validity_rate": 0.0,
            "rule_failures": {},
        }

        failures = evaluate_quality_gate(
            quality_report=report,
            contract=self.contract,
        )

        self.assertTrue(failures)

    def test_all_quality_rules_use_rule_id_key(self):
        contract = {
            "quality_rules": [
                {
                    "rule_id": "DQ009",
                    "severity": "high",
                }
            ],
            "quality_gate": {
                "minimum_validity_rate": 0.85,
                "maximum_high_severity_failure_rate": 0.1,
            },
        }

        report = {
            "input_rows": 100,
            "validity_rate": 0.95,
            "rule_failures": {
                "DQ009_REQUIRED_TRANSACTION_ID": 11,
            },
        }

        failures = evaluate_quality_gate(
            quality_report=report,
            contract=contract,
        )

        self.assertEqual(len(failures), 1)
        self.assertIn(
            "DQ009_REQUIRED_TRANSACTION_ID",
            failures[0],
        )


if __name__ == "__main__":
    unittest.main()

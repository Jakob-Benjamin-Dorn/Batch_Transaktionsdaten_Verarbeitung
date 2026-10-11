from __future__ import annotations

import unittest

from src.ai_summary.prepare_payload import (
    build_ai_payload,
    find_forbidden_keys,
)


class TestAiPayload(unittest.TestCase):
    def setUp(self):
        self.quality_report = {
            "logical_date": "2026-01-01",
            "status": "warning",
            "input_rows": 100,
            "valid_rows": 90,
            "quarantined_rows": 10,
            "validity_rate": 0.9,
            "rule_failures": {
                "DQ001_REQUIRED_CUSTOMER_ID": 10,
            },
            "customer_id": "must-not-leave",
        }

        self.business_metrics = {
            "logical_date": "2026-01-01",
            "daily_kpis": {
                "logical_date": "2026-01-01",
                "total_transactions": 90,
                "approved_transactions": 80,
                "fraud_rate": 0.03,
                "transaction_id": "must-not-leave",
            },
            "country_currency_summary": [
                {
                    "logical_date": "2026-01-01",
                    "transaction_country": "DE",
                    "currency": "EUR",
                    "transaction_count": 70,
                    "approved_count": 65,
                    "fraud_count": 2,
                    "net_amount": 5000.0,
                    "customer_id": "must-not-leave",
                }
            ],
        }

        self.contract = {
            "contract_name": ("transaction_bronze_contract"),
            "contract_version": "1.0.0",
            "description": "Test contract",
            "fields": [
                {
                    "name": "currency",
                    "target_type": "string",
                    "nullable": False,
                    "allowed_values": [
                        "EUR",
                        "USD",
                    ],
                    "description": "Currency code",
                    "example_customer_id": "not allowed",
                }
            ],
            "quality_rules": [
                {
                    "rule_id": "DQ002",
                    "name": "valid_currency",
                    "description": ("Currency must be allowed."),
                    "severity": "medium",
                    "internal_owner": "secret-team",
                }
            ],
        }

    def test_payload_contains_only_allowed_fields(self):
        payload = build_ai_payload(
            quality_report=self.quality_report,
            business_metrics=self.business_metrics,
            contract=self.contract,
        )

        self.assertEqual(
            find_forbidden_keys(payload),
            [],
        )

        self.assertNotIn(
            "customer_id",
            payload["quality_report"],
        )

        self.assertNotIn(
            "transaction_id",
            payload["business_metrics"]["daily_kpis"],
        )

        first_country = payload["business_metrics"]["country_currency_summary"][0]

        self.assertNotIn(
            "customer_id",
            first_country,
        )

    def test_allowed_aggregates_are_preserved(self):
        payload = build_ai_payload(
            quality_report=self.quality_report,
            business_metrics=self.business_metrics,
            contract=self.contract,
        )

        self.assertEqual(
            payload["quality_report"]["input_rows"],
            100,
        )

        self.assertEqual(
            payload["business_metrics"]["daily_kpis"]["total_transactions"],
            90,
        )

        self.assertEqual(
            payload["business_metrics"]["country_currency_summary"][0][
                "transaction_country"
            ],
            "DE",
        )

        self.assertTrue(payload["pipeline_capabilities"]["duplicates_are_detected"])

        self.assertTrue(
            payload["pipeline_capabilities"][
                "amounts_are_not_converted_to_one_currency"
            ]
        )

    def test_forbidden_key_detection_is_recursive(self):
        unsafe_payload = {"safe": {"nested": [{"customer_id": "cus_123"}]}}

        violations = find_forbidden_keys(unsafe_payload)

        self.assertEqual(
            violations,
            [("payload.safe.nested[0].customer_id")],
        )


if __name__ == "__main__":
    unittest.main()

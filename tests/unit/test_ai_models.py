from __future__ import annotations

import unittest

from pydantic import ValidationError

from src.ai_summary.models import AiBatchSummary


class TestAiModels(unittest.TestCase):
    def valid_summary(self) -> dict:
        return {
            "logical_date": "2026-01-01",
            "overall_status": "warning",
            "executive_summary": (
                "Der Batch wurde erfolgreich verarbeitet. "
                "Ein Teil der Eingabedaten wurde aufgrund "
                "von Qualitätsproblemen quarantänisiert."
            ),
            "findings": [
                {
                    "rule_id": (
                        "DQ001_REQUIRED_CUSTOMER_ID"
                    ),
                    "severity": "high",
                    "title": (
                        "Fehlende Kundenzuordnung"
                    ),
                    "observation": (
                        "Ein Teil der Transaktionen "
                        "enthielt keine customer_id."
                    ),
                    "business_impact": (
                        "Diese Transaktionen können nicht "
                        "eindeutig einem Kunden zugeordnet "
                        "werden."
                    ),
                    "recommended_action": (
                        "Die Kundenreferenzen im "
                        "Quellsystem prüfen."
                    ),
                }
            ],
            "business_highlights": [
                "Die Approval-Rate lag bei 92 Prozent."
            ],
            "recommended_actions": [
                "Die häufigsten DQ-Regeln untersuchen."
            ],
        }

    def test_valid_summary_is_accepted(self):
        summary = AiBatchSummary.model_validate(
            self.valid_summary()
        )

        self.assertEqual(
            summary.overall_status,
            "warning",
        )

        self.assertEqual(
            len(summary.findings),
            1,
        )

    def test_invalid_status_is_rejected(self):
        value = self.valid_summary()
        value["overall_status"] = "maybe"

        with self.assertRaises(
            ValidationError
        ):
            AiBatchSummary.model_validate(value)

    def test_missing_required_field_is_rejected(self):
        value = self.valid_summary()
        del value["executive_summary"]

        with self.assertRaises(
            ValidationError
        ):
            AiBatchSummary.model_validate(value)

    def test_unknown_field_is_rejected(self):
        value = self.valid_summary()
        value["unexpected_field"] = "not allowed"

        with self.assertRaises(
            ValidationError
        ):
            AiBatchSummary.model_validate(value)

    def test_invalid_finding_severity_is_rejected(self):
        value = self.valid_summary()
        value["findings"][0]["severity"] = "extreme"

        with self.assertRaises(
            ValidationError
        ):
            AiBatchSummary.model_validate(value)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.ai_summary.create_ai_summary import (
    parse_and_validate_summary,
    validate_finding_rule_ids,
    write_summary,
)


class TestCreateAiSummary(unittest.TestCase):
    def valid_response(self) -> str:
        return json.dumps(
            {
                "logical_date": "2026-01-01",
                "overall_status": "warning",
                "executive_summary": ("Der Batch wurde verarbeitet."),
                "findings": [
                    {
                        "rule_id": ("DQ001_REQUIRED_CUSTOMER_ID"),
                        "severity": "high",
                        "title": ("Fehlende Kundenzuordnung"),
                        "observation": ("DQ001 wurde verletzt."),
                        "business_impact": ("Ein Teil der Daten ist nicht zuordenbar."),
                        "recommended_action": ("Die Datenquelle prüfen."),
                    }
                ],
                "business_highlights": ["Die Approval-Rate war stabil."],
                "recommended_actions": ["DQ001 untersuchen."],
            }
        )

    def test_valid_json_response_is_accepted(self):
        summary = parse_and_validate_summary(
            raw_content=self.valid_response(),
            expected_logical_date="2026-01-01",
        )

        self.assertEqual(
            summary.overall_status,
            "warning",
        )

    def test_invalid_json_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_and_validate_summary(
                raw_content="not-json",
                expected_logical_date=("2026-01-01"),
            )

    def test_shortened_rule_id_is_rejected(self):
        summary = parse_and_validate_summary(
            raw_content=self.valid_response(),
            expected_logical_date="2026-01-01",
        )

        payload = {
            "quality_report": {"rule_failures": {"DQ001_REQUIRED_CUSTOMER_ID": 10}}
        }

        summary.findings[0].rule_id = "DQ001"

        with self.assertRaises(ValueError):
            validate_finding_rule_ids(
                summary=summary,
                payload=payload,
            )

    def test_wrong_logical_date_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_and_validate_summary(
                raw_content=self.valid_response(),
                expected_logical_date=("2026-01-02"),
            )

    def test_summary_file_contains_metadata(self):
        summary = parse_and_validate_summary(
            raw_content=self.valid_response(),
            expected_logical_date="2026-01-01",
        )

        with tempfile.TemporaryDirectory() as temp:
            output_path = Path(temp) / "ai_summary.json"

            write_summary(
                summary=summary,
                output_path=output_path,
                model="test-model",
                usage={
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "total_tokens": 150,
                },
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(
            result["provider"],
            "deepseek",
        )
        self.assertEqual(
            result["model"],
            "test-model",
        )
        self.assertEqual(
            result["usage"]["total_tokens"],
            150,
        )


if __name__ == "__main__":
    unittest.main()

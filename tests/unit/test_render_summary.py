from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.ai_summary.render_summary import (
    load_ai_summary,
    render_markdown,
    write_markdown,
)


class TestRenderSummary(unittest.TestCase):
    def setUp(self):
        self.template_dir = (
            Path("/opt/airflow/src/ai_summary/templates")
        )

        self.document = {
            "provider": "deepseek",
            "model": "test-model",
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150,
            },
            "summary": {
                "logical_date": "2026-01-01",
                "overall_status": "warning",
                "executive_summary": (
                    "Der Batch wurde verarbeitet."
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
                            "50 Zeilen verletzen die Regel."
                        ),
                        "business_impact": (
                            "Die betroffenen Datensätze "
                            "sind nicht eindeutig zuordenbar."
                        ),
                        "recommended_action": (
                            "Die Ursache im Quellsystem "
                            "untersuchen."
                        ),
                    }
                ],
                "business_highlights": [
                    "Die Approval-Rate beträgt 92 Prozent."
                ],
                "recommended_actions": [
                    "Die häufigsten Regelverletzungen untersuchen."
                ],
            },
        }

    def test_markdown_contains_expected_sections(self):
        markdown = render_markdown(
            document=self.document,
            template_dir=self.template_dir,
        )

        self.assertIn(
            "# Batch-Zusammenfassung – 2026-01-01",
            markdown,
        )

        self.assertIn(
            "## Data-Quality-Findings",
            markdown,
        )

        self.assertIn(
            "DQ001_REQUIRED_CUSTOMER_ID",
            markdown,
        )

        self.assertIn(
            "## Business Highlights",
            markdown,
        )

        self.assertIn(
            "deepseek",
            markdown,
        )

    def test_markdown_file_is_written(self):
        markdown = render_markdown(
            document=self.document,
            template_dir=self.template_dir,
        )

        with tempfile.TemporaryDirectory() as temp:
            output_path = (
                Path(temp)
                / "ai_summary.md"
            )

            write_markdown(
                markdown=markdown,
                output_path=output_path,
            )

            self.assertTrue(
                output_path.is_file()
            )

            content = output_path.read_text(
                encoding="utf-8"
            )

            self.assertEqual(
                content,
                markdown,
            )

    def test_summary_json_is_loaded_and_validated(self):
        with tempfile.TemporaryDirectory() as temp:
            input_path = (
                Path(temp)
                / "ai_summary.json"
            )

            input_path.write_text(
                json.dumps(
                    self.document,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = load_ai_summary(
                input_path
            )

            self.assertEqual(
                result["model"],
                "test-model",
            )

    def test_missing_metadata_is_rejected(self):
        invalid_document = {
            "summary": self.document["summary"]
        }

        with tempfile.TemporaryDirectory() as temp:
            input_path = (
                Path(temp)
                / "ai_summary.json"
            )

            input_path.write_text(
                json.dumps(
                    invalid_document,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaises(
                ValueError
            ):
                load_ai_summary(input_path)


if __name__ == "__main__":
    unittest.main()

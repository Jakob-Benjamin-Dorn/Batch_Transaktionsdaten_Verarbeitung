from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.common.dates import validate_logical_date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate the daily quality report against "
            "the configured quality-gate thresholds."
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
        "--contract-path",
        default=("/opt/airflow/knowledge_base/data_contract.json"),
        help="Path to the data contract.",
    )

    return parser.parse_args()


def report_path(
    data_dir: str,
    logical_date: str,
) -> Path:
    batch_date = datetime.strptime(
        logical_date,
        "%Y-%m-%d",
    )

    return (
        Path(data_dir)
        / "reports"
        / f"year={batch_date.year:04d}"
        / f"month={batch_date.month:02d}"
        / f"day={batch_date.day:02d}"
        / "quality_report.json"
    )


def load_json(
    path: Path,
) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Required JSON file does not exist: {path}")

    with path.open(
        encoding="utf-8",
    ) as file:
        value = json.load(file)

    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")

    return value


def rule_severity_by_id(
    contract: dict[str, Any],
) -> dict[str, str]:
    result = {}

    for rule in contract.get(
        "quality_rules",
        [],
    ):
        if not isinstance(rule, dict):
            continue

        rule_id = rule.get("rule_id")
        severity = rule.get("severity")

        if rule_id and severity:
            result[str(rule_id)] = str(severity)

    return result


def normalize_report_rule_id(
    report_rule_id: str,
) -> str:
    """
    Convert report IDs such as
    DQ001_REQUIRED_CUSTOMER_ID
    to the contract ID DQ001.
    """
    return report_rule_id.split(
        "_",
        maxsplit=1,
    )[0]


def evaluate_quality_gate(
    quality_report: dict[str, Any],
    contract: dict[str, Any],
) -> list[str]:
    gate_config = contract.get(
        "quality_gate",
        {},
    )

    if not isinstance(gate_config, dict):
        raise ValueError("quality_gate must be an object.")

    minimum_validity_rate = float(gate_config["minimum_validity_rate"])

    maximum_high_failure_rate = float(gate_config["maximum_high_severity_failure_rate"])

    input_rows = int(
        quality_report.get(
            "input_rows",
            0,
        )
    )

    validity_rate = float(
        quality_report.get(
            "validity_rate",
            0.0,
        )
    )

    rule_failures = quality_report.get(
        "rule_failures",
        {},
    )

    if not isinstance(rule_failures, dict):
        raise ValueError("rule_failures must be an object.")

    failures = []

    if input_rows <= 0:
        failures.append("The batch contains no input rows.")

    if validity_rate < minimum_validity_rate:
        failures.append(
            "Validity rate "
            f"{validity_rate:.2%} is below "
            f"the minimum of "
            f"{minimum_validity_rate:.2%}."
        )

    severities = rule_severity_by_id(contract)

    if input_rows > 0:
        for report_rule_id, failure_count in rule_failures.items():
            contract_rule_id = normalize_report_rule_id(str(report_rule_id))

            severity = severities.get(contract_rule_id)

            if severity != "high":
                continue

            failure_rate = int(failure_count) / input_rows

            if failure_rate > maximum_high_failure_rate:
                failures.append(
                    f"{report_rule_id} affects "
                    f"{failure_rate:.2%} of input rows, "
                    "above the allowed maximum of "
                    f"{maximum_high_failure_rate:.2%}."
                )

    return failures


def main() -> None:
    args = parse_args()
    validate_logical_date(args.logical_date)

    quality_report = load_json(
        report_path(
            data_dir=args.data_dir,
            logical_date=args.logical_date,
        )
    )

    contract = load_json(Path(args.contract_path))

    failures = evaluate_quality_gate(
        quality_report=quality_report,
        contract=contract,
    )

    if failures:
        print("Quality gate FAILED.")

        for failure in failures:
            print(f"- {failure}")

        raise SystemExit(1)

    print("Quality gate PASSED.")
    print(
        "Validity rate:",
        f"{quality_report['validity_rate']:.2%}",
    )


if __name__ == "__main__":
    main()

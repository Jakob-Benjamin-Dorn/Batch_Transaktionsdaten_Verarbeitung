from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.common.dates import validate_logical_date

ALLOWED_QUALITY_REPORT_FIELDS = {
    "logical_date",
    "status",
    "input_rows",
    "valid_rows",
    "quarantined_rows",
    "validity_rate",
    "rule_failures",
}

ALLOWED_DAILY_KPI_FIELDS = {
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
}

ALLOWED_COUNTRY_SUMMARY_FIELDS = {
    "logical_date",
    "transaction_country",
    "currency",
    "transaction_count",
    "approved_count",
    "fraud_count",
    "net_amount",
}

FORBIDDEN_KEYS = {
    "transaction_id",
    "customer_id",
    "merchant_id",
    "device_id",
    "transaction_city",
    "event_time",
    "ingestion_time",
    "raw_payload",
}

ALLOWED_CONTRACT_FIELD_KEYS = {
    "name",
    "target_type",
    "nullable",
    "optional",
    "allowed_values",
    "description",
}

ALLOWED_CONTRACT_RULE_KEYS = {
    "rule_id",
    "name",
    "description",
    "severity",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare a privacy-safe, aggregated payload "
            "for the AI summary."
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
        default=(
            "/opt/airflow/knowledge_base/"
            "data_contract.json"
        ),
        help="Path to the data contract.",
    )

    return parser.parse_args()


def report_directory(
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
    )


def load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Required JSON file does not exist: {path}"
        )

    with path.open(
        encoding="utf-8",
    ) as file:
        value = json.load(file)

    if not isinstance(value, dict):
        raise ValueError(
            f"Expected a JSON object in {path}"
        )

    return value


def select_allowed_fields(
    source: dict[str, Any],
    allowed_fields: set[str],
) -> dict[str, Any]:
    return {
        key: source[key]
        for key in allowed_fields
        if key in source
    }


def sanitize_quality_report(
    report: dict[str, Any],
) -> dict[str, Any]:
    return select_allowed_fields(
        source=report,
        allowed_fields=ALLOWED_QUALITY_REPORT_FIELDS,
    )


def sanitize_daily_kpis(
    daily_kpis: dict[str, Any],
) -> dict[str, Any]:
    return select_allowed_fields(
        source=daily_kpis,
        allowed_fields=ALLOWED_DAILY_KPI_FIELDS,
    )


def sanitize_country_summary(
    rows: Any,
) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        raise ValueError(
            "country_currency_summary must be a list."
        )

    sanitized_rows = []

    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(
                "Each country summary item must be an object."
            )

        sanitized_rows.append(
            select_allowed_fields(
                source=row,
                allowed_fields=(
                    ALLOWED_COUNTRY_SUMMARY_FIELDS
                ),
            )
        )

    return sanitized_rows


def sanitize_business_metrics(
    metrics: dict[str, Any],
) -> dict[str, Any]:
    daily_kpis = metrics.get("daily_kpis", {})
    country_summary = metrics.get(
        "country_currency_summary",
        [],
    )

    if not isinstance(daily_kpis, dict):
        raise ValueError(
            "daily_kpis must be a JSON object."
        )

    return {
        "logical_date": metrics.get("logical_date"),
        "daily_kpis": sanitize_daily_kpis(
            daily_kpis
        ),
        "country_currency_summary": (
            sanitize_country_summary(
                country_summary
            )
        ),
    }


def sanitize_contract(
    contract: dict[str, Any],
) -> dict[str, Any]:
    fields = contract.get("fields", [])
    quality_rules = contract.get(
        "quality_rules",
        [],
    )

    sanitized_fields = [
        select_allowed_fields(
            source=field,
            allowed_fields=ALLOWED_CONTRACT_FIELD_KEYS,
        )
        for field in fields
        if isinstance(field, dict)
    ]

    sanitized_rules = [
        select_allowed_fields(
            source=rule,
            allowed_fields=ALLOWED_CONTRACT_RULE_KEYS,
        )
        for rule in quality_rules
        if isinstance(rule, dict)
    ]

    return {
        "contract_name": contract.get(
            "contract_name"
        ),
        "contract_version": contract.get(
            "contract_version"
        ),
        "description": contract.get(
            "description"
        ),
        "fields": sanitized_fields,
        "quality_rules": sanitized_rules,
    }


def find_forbidden_keys(
    value: Any,
    current_path: str = "payload",
) -> list[str]:
    violations = []

    if isinstance(value, dict):
        for key, item in value.items():
            item_path = f"{current_path}.{key}"

            if key in FORBIDDEN_KEYS:
                violations.append(item_path)

            violations.extend(
                find_forbidden_keys(
                    item,
                    item_path,
                )
            )

    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(
                find_forbidden_keys(
                    item,
                    f"{current_path}[{index}]",
                )
            )

    return violations


def build_ai_payload(
    quality_report: dict[str, Any],
    business_metrics: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "pipeline_capabilities": {
            "invalid_rows_are_quarantined": True,
            "duplicates_are_detected": True,
            "duplicate_resolution": (
                "Keep the record with the newest ingestion_time "
                "and quarantine older duplicates."
            ),
            "silver_contains_only_valid_rows": True,
            "amounts_are_not_converted_to_one_currency": True,
        },
        "quality_report": sanitize_quality_report(
            quality_report
        ),
        "business_metrics": sanitize_business_metrics(
            business_metrics
        ),
        "data_contract": sanitize_contract(
            contract
        ),
    }

    violations = find_forbidden_keys(payload)

    if violations:
        formatted = ", ".join(violations)

        raise ValueError(
            "Forbidden fields detected in AI payload: "
            f"{formatted}"
        )

    return payload


def write_payload(
    payload: dict[str, Any],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            payload,
            file,
            indent=2,
            ensure_ascii=False,
        )


def main() -> None:
    args = parse_args()
    validate_logical_date(args.logical_date)

    reports_dir = report_directory(
        data_dir=args.data_dir,
        logical_date=args.logical_date,
    )

    quality_report = load_json(
        reports_dir / "quality_report.json"
    )

    business_metrics = load_json(
        reports_dir / "business_metrics.json"
    )

    contract = load_json(
        Path(args.contract_path)
    )

    payload = build_ai_payload(
        quality_report=quality_report,
        business_metrics=business_metrics,
        contract=contract,
    )

    output_path = (
        reports_dir
        / "ai_input_payload.json"
    )

    write_payload(
        payload=payload,
        output_path=output_path,
    )

    print("AI input payload created.")
    print(f"Logical date: {args.logical_date}")
    print(f"Output path:  {output_path}")


if __name__ == "__main__":
    main()
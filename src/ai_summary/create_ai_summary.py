from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import APIConnectionError
from openai import APIStatusError
from openai import OpenAI
from pydantic import ValidationError

from src.ai_summary.models import AiBatchSummary
from src.ai_summary.prompts import (
    SYSTEM_PROMPT,
    build_user_prompt,
)


DEFAULT_MODEL = "deepseek-v4-flash"
DEFAULT_BASE_URL = "https://api.deepseek.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a structured AI summary from the "
            "privacy-safe aggregated payload."
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

    return parser.parse_args()


def validate_logical_date(
    logical_date: str,
) -> None:
    try:
        datetime.strptime(
            logical_date,
            "%Y-%m-%d",
        )
    except ValueError as exc:
        raise ValueError(
            "--logical-date must use the format YYYY-MM-DD"
        ) from exc


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


def load_payload(
    path: Path,
) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(
            f"AI input payload does not exist: {path}"
        )

    with path.open(
        encoding="utf-8",
    ) as file:
        payload = json.load(file)

    if not isinstance(payload, dict):
        raise ValueError(
            "AI input payload must be a JSON object."
        )

    return payload


def create_client(
    api_key: str,
    base_url: str = DEFAULT_BASE_URL,
) -> OpenAI:
    return OpenAI(
        api_key=api_key,
        base_url=base_url,
        timeout=60.0,
        max_retries=2,
    )


def request_summary(
    client: OpenAI,
    model: str,
    payload: dict[str, Any],
) -> tuple[str, dict[str, int | None]]:
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": build_user_prompt(
                    payload
                ),
            },
        ],
        response_format={
            "type": "json_object",
        },
        max_tokens=2500,
        stream=False,
        extra_body={
            "thinking": {
                "type": "disabled",
            }
        },
    )

    content = response.choices[0].message.content

    if not content or not content.strip():
        raise ValueError(
            "DeepSeek returned an empty response."
        )

    usage = {
        "prompt_tokens": (
            response.usage.prompt_tokens
            if response.usage
            else None
        ),
        "completion_tokens": (
            response.usage.completion_tokens
            if response.usage
            else None
        ),
        "total_tokens": (
            response.usage.total_tokens
            if response.usage
            else None
        ),
    }

    return content, usage


def parse_and_validate_summary(
    raw_content: str,
    expected_logical_date: str,
) -> AiBatchSummary:
    try:
        parsed = json.loads(raw_content)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "DeepSeek response is not valid JSON."
        ) from exc

    try:
        summary = AiBatchSummary.model_validate(
            parsed
        )
    except ValidationError as exc:
        raise ValueError(
            "DeepSeek response does not match "
            "the expected summary schema."
        ) from exc

    if summary.logical_date != expected_logical_date:
        raise ValueError(
            "DeepSeek returned an unexpected logical_date: "
            f"{summary.logical_date}"
        )

    return summary


def validate_finding_rule_ids(
    summary: AiBatchSummary,
    payload: dict[str, Any],
) -> None:
    rule_failures = (
        payload
        .get("quality_report", {})
        .get("rule_failures", {})
    )

    expected_rule_ids = {
        rule_id
        for rule_id, failure_count
        in rule_failures.items()
        if failure_count > 0
    }

    returned_rule_ids = {
        finding.rule_id
        for finding in summary.findings
    }

    unknown_rule_ids = (
        returned_rule_ids - expected_rule_ids
    )

    if unknown_rule_ids:
        raise ValueError(
            "AI summary contains unknown or shortened "
            "rule IDs: "
            + ", ".join(
                sorted(unknown_rule_ids)
            )
        )


def write_summary(
    summary: AiBatchSummary,
    output_path: Path,
    model: str,
    usage: dict[str, int | None],
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload = {
        "provider": "deepseek",
        "model": model,
        "usage": usage,
        "summary": summary.model_dump(),
    }

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


SEVERITY_ORDER = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
    "info": 4,
}


def sort_findings(
    summary: AiBatchSummary,
    payload: dict[str, Any],
) -> None:
    rule_failures = (
        payload
        .get("quality_report", {})
        .get("rule_failures", {})
    )

    summary.findings.sort(
        key=lambda finding: (
            SEVERITY_ORDER[finding.severity],
            -rule_failures.get(
                finding.rule_id,
                0,
            ),
        )
    )


def main() -> None:
    args = parse_args()
    validate_logical_date(args.logical_date)

    api_key = os.getenv("DEEPSEEK_API_KEY")

    if not api_key:
        raise RuntimeError(
            "DEEPSEEK_API_KEY is not configured."
        )

    model = os.getenv(
        "DEEPSEEK_MODEL",
        DEFAULT_MODEL,
    )

    base_url = os.getenv(
        "DEEPSEEK_BASE_URL",
        DEFAULT_BASE_URL,
    )

    reports_dir = report_directory(
        data_dir=args.data_dir,
        logical_date=args.logical_date,
    )

    input_path = (
        reports_dir
        / "ai_input_payload.json"
    )

    output_path = (
        reports_dir
        / "ai_summary.json"
    )

    payload = load_payload(input_path)

    client = create_client(
        api_key=api_key,
        base_url=base_url,
    )

    try:
        raw_content, usage = request_summary(
            client=client,
            model=model,
            payload=payload,
        )

        summary = parse_and_validate_summary(
            raw_content=raw_content,
            expected_logical_date=(
                args.logical_date
            ),
        )

        validate_finding_rule_ids(
            summary=summary,
            payload=payload,
        )

        validate_finding_rule_ids(
            summary=summary,
            payload=payload,
        )

        sort_findings(
            summary=summary,
            payload=payload,
        )

        write_summary(
            summary=summary,
            output_path=output_path,
            model=model,
            usage=usage,
        )

    except APIConnectionError as exc:
        raise RuntimeError(
            "Could not connect to the DeepSeek API."
        ) from exc

    except APIStatusError as exc:
        raise RuntimeError(
            "DeepSeek API returned an error "
            f"with status {exc.status_code}."
        ) from exc

    print("AI summary created.")
    print(f"Logical date: {args.logical_date}")
    print(f"Model:        {model}")
    print(f"Output path:  {output_path}")
    print(f"Token usage:  {usage}")


if __name__ == "__main__":
    main()

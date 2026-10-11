from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from src.ai_summary.models import AiBatchSummary
from src.common.dates import validate_logical_date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Render the validated AI summary JSON as deterministic Markdown.")
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
        "--template-dir",
        default=("/opt/airflow/src/ai_summary/templates"),
        help="Directory containing Jinja2 templates.",
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


def load_ai_summary(
    path: Path,
) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"AI summary file does not exist: {path}")

    with path.open(
        encoding="utf-8",
    ) as file:
        value = json.load(file)

    if not isinstance(value, dict):
        raise ValueError("AI summary file must contain a JSON object.")

    required_fields = {
        "provider",
        "model",
        "usage",
        "summary",
    }

    missing_fields = required_fields - value.keys()

    if missing_fields:
        raise ValueError(
            "AI summary file is missing fields: " + ", ".join(sorted(missing_fields))
        )

    if not isinstance(value["usage"], dict):
        raise ValueError("usage must be a JSON object.")

    AiBatchSummary.model_validate(value["summary"])

    return value


def create_environment(
    template_dir: Path,
) -> Environment:
    if not template_dir.is_dir():
        raise FileNotFoundError(f"Template directory does not exist: {template_dir}")

    return Environment(
        loader=FileSystemLoader(str(template_dir)),
        undefined=StrictUndefined,
        autoescape=select_autoescape(
            enabled_extensions=(),
            default_for_string=False,
            default=False,
        ),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def render_markdown(
    document: dict[str, Any],
    template_dir: Path,
) -> str:
    summary = AiBatchSummary.model_validate(document["summary"])

    environment = create_environment(template_dir)

    template = environment.get_template("summary.md.j2")

    return (
        template.render(
            provider=document["provider"],
            model=document["model"],
            usage=document["usage"],
            summary=summary.model_dump(),
        ).strip()
        + "\n"
    )


def write_markdown(
    markdown: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path.write_text(
        markdown,
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    validate_logical_date(args.logical_date)

    reports_dir = report_directory(
        data_dir=args.data_dir,
        logical_date=args.logical_date,
    )

    input_path = reports_dir / "ai_summary.json"
    output_path = reports_dir / "ai_summary.md"

    document = load_ai_summary(input_path)

    markdown = render_markdown(
        document=document,
        template_dir=Path(args.template_dir),
    )

    write_markdown(
        markdown=markdown,
        output_path=output_path,
    )

    print("Markdown summary rendered.")
    print(f"Logical date: {args.logical_date}")
    print(f"Input path:   {input_path}")
    print(f"Output path:  {output_path}")


if __name__ == "__main__":
    main()

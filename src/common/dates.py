from __future__ import annotations

from datetime import datetime

LOGICAL_DATE_FORMAT = "%Y-%m-%d"


def validate_logical_date(logical_date: str) -> None:
    """Raise ValueError if logical_date is not a valid YYYY-MM-DD date."""
    try:
        datetime.strptime(logical_date, LOGICAL_DATE_FORMAT)
    except ValueError as exc:
        raise ValueError("--logical-date must use the format YYYY-MM-DD") from exc

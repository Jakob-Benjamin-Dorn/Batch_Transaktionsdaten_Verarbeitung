from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Severity = Literal[
    "info",
    "low",
    "medium",
    "high",
    "critical",
]

OverallStatus = Literal[
    "healthy",
    "warning",
    "failed",
]


class QualityFinding(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
    )

    rule_id: str = Field(
        min_length=1,
        description=(
            "Identifier of the referenced data-quality rule."
        ),
    )

    severity: Severity

    title: str = Field(
        min_length=1,
        max_length=120,
    )

    observation: str = Field(
        min_length=1,
        max_length=600,
    )

    business_impact: str = Field(
        min_length=1,
        max_length=600,
    )

    recommended_action: str = Field(
        min_length=1,
        max_length=600,
    )


class AiBatchSummary(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
    )

    logical_date: str = Field(
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )

    overall_status: OverallStatus

    executive_summary: str = Field(
        min_length=1,
        max_length=1200,
    )

    findings: list[QualityFinding] = Field(
        default_factory=list,
        max_length=10,
    )

    business_highlights: list[str] = Field(
        default_factory=list,
        max_length=10,
    )

    recommended_actions: list[str] = Field(
        default_factory=list,
        max_length=10,
    )

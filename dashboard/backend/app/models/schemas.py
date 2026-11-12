"""Request models for the dashboard API."""

from typing import Literal

from pydantic import BaseModel, Field


ScenarioName = Literal[
    "normal_flow",
    "duplicate_events",
    "late_events",
    "out_of_order_events",
    "invalid_events",
    "continuous",
]


class GeneratorRunRequest(BaseModel):
    scenario: ScenarioName = "normal_flow"
    interval: float = Field(default=2.0, gt=0, le=60)


class OperationResponse(BaseModel):
    ok: bool
    status: str
    message: str

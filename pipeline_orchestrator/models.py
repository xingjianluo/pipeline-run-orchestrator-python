"""管道编排领域模型：状态枚举、步骤、运行记录、事件。"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


_TERMINAL_RUN_STATUSES = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}
)


def is_terminal(status: RunStatus) -> bool:
    return status in _TERMINAL_RUN_STATUSES


class FailureReason(str, Enum):
    NONE = "none"
    STEP_ERROR = "step_error"
    UPSTREAM_FAILED = "upstream_failed"
    UPSTREAM_SKIPPED = "upstream_skipped"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


@dataclass
class StepDef:
    name: str
    upstream: tuple[str, ...] = ()


@dataclass
class StepState:
    name: str
    status: StepStatus = StepStatus.PENDING
    reason: FailureReason = FailureReason.NONE
    detail: str = ""
    started_at: str = ""
    ended_at: str = ""
    attempts: int = 0
    output_name: str = ""
    output_version: str = ""


@dataclass
class RunEvent:
    seq: int
    run_id: str
    step: str
    from_status: str
    to_status: str
    reason: str
    at: str
    detail: str = ""


@dataclass
class PipelineRun:
    run_id: str
    pipeline_name: str
    trigger: str
    status: RunStatus
    input_versions: dict[str, str]
    steps: dict[str, StepState]
    events: list[RunEvent] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""


@dataclass
class Lineage:
    """一次运行的血缘视图：输入版本、产物版本、触发来源与运行结论。"""

    run_id: str
    pipeline_name: str
    trigger: str
    status: str
    input_versions: dict[str, str]
    outputs: dict[str, str]  # step name -> output version
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "trigger": self.trigger,
            "status": self.status,
            "input_versions": dict(self.input_versions),
            "outputs": dict(self.outputs),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


def step_to_dict(step: StepState) -> dict[str, Any]:
    return {
        "name": step.name,
        "status": step.status.value,
        "reason": step.reason.value,
        "detail": step.detail,
        "attempts": step.attempts,
        "started_at": step.started_at,
        "ended_at": step.ended_at,
        "output_name": step.output_name,
        "output_version": step.output_version,
    }


def event_to_dict(event: RunEvent) -> dict[str, Any]:
    return {
        "seq": event.seq,
        "run_id": event.run_id,
        "step": event.step,
        "from_status": event.from_status,
        "to_status": event.to_status,
        "reason": event.reason,
        "at": event.at,
        "detail": event.detail,
    }


def run_to_dict(run: PipelineRun) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "pipeline_name": run.pipeline_name,
        "trigger": run.trigger,
        "status": run.status.value,
        "input_versions": dict(run.input_versions),
        "steps": {name: step_to_dict(s) for name, s in run.steps.items()},
        "events": [event_to_dict(e) for e in run.events],
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    }


def snapshot_run(run: PipelineRun) -> PipelineRun:
    """深拷贝一份运行记录，避免外部持有内部可变状态。"""
    return copy.deepcopy(run)

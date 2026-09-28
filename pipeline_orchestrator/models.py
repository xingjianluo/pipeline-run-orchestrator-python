"""领域模型：管道、步骤、运行、血缘的数据结构与状态枚举。"""
from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from typing import Any


def new_id(prefix: str) -> str:
    """生成带前缀的标识，例如 run_xxx / steprun_xxx。"""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class RunStatus(str, enum.Enum):
    """一次管道运行的状态。"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class StepStatus(str, enum.Enum):
    """运行中单个步骤的状态。"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


class FailureReason(str, enum.Enum):
    """可区分的失败原因。"""
    NONE = "none"
    STEP_EXCEPTION = "step_exception"
    UPSTREAM_FAILED = "upstream_failed"
    STEP_TIMEOUT = "step_timeout"
    RUN_TIMEOUT = "run_timeout"
    CANCELLED = "cancelled"


@dataclass
class StepDef:
    """管道中的一个步骤定义。"""
    name: str
    upstream: list[str] = field(default_factory=list)
    timeout_seconds: float | None = None
    # 步骤执行体由注册表注入，骨架阶段仅占位
    handler: Any = field(default=None, repr=False)


@dataclass
class PipelineDef:
    """管道定义：名字、版本、有向无环步骤图。"""
    name: str
    version: str
    steps: list[StepDef] = field(default_factory=list)

    def step(self, name: str) -> StepDef | None:
        for s in self.steps:
            if s.name == name:
                return s
        return None


@dataclass
class StepRun:
    """某一步在某次运行中的执行记录（只追加、不就地覆盖）。"""
    id: str
    run_id: str
    step_name: str
    status: StepStatus = StepStatus.PENDING
    attempt: int = 0
    failure_reason: FailureReason = FailureReason.NONE
    error_message: str = ""
    started_at: str = ""
    finished_at: str = ""
    # 该步骤消费的输入版本与产出的输出版本
    input_versions: dict[str, str] = field(default_factory=dict)
    output_ref: str = ""
    output_version: str = ""


@dataclass
class PipelineRun:
    """一次管道运行的聚合根。"""
    id: str
    pipeline_name: str
    pipeline_version: str
    status: RunStatus = RunStatus.PENDING
    trigger: str = "manual"
    parent_run_id: str = ""
    created_at: str = ""
    started_at: str = ""
    finished_at: str = ""
    timeout_seconds: float | None = None
    failure_reason: FailureReason = FailureReason.NONE
    error_message: str = ""
    steps: dict[str, StepRun] = field(default_factory=dict)
    # 本次运行请求的输入引用 -> 输入版本快照
    input_versions: dict[str, str] = field(default_factory=dict)

    def is_terminal(self) -> bool:
        return self.status in TERMINAL_RUN_STATUSES


@dataclass
class LineageRecord:
    """血缘记录：一次运行用了什么版本的输入、产出了什么、由谁触发。"""
    run_id: str
    pipeline_name: str
    pipeline_version: str
    trigger: str
    parent_run_id: str
    status: str
    input_versions: dict[str, str]
    outputs: list[dict[str, str]]
    created_at: str
    finished_at: str


def make_run_id() -> str:
    return new_id("run")


def make_step_run_id() -> str:
    return new_id("steprun")


TERMINAL_RUN_STATUSES = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}
)

"""测试辅助：构造隔离的编排器实例，并统一打印判定依据。"""
from __future__ import annotations

import logging

from pipeline_orchestrator import service
from pipeline_orchestrator.executor import Orchestrator
from pipeline_orchestrator.models import FailureReason, PipelineRun, RunStatus, StepStatus

logger = logging.getLogger("test.assertions")


def fresh_orchestrator(timeout: float = 5.0) -> Orchestrator:
    service.reset_scenarios()
    orch = service.build_orchestrator(default_timeout=timeout)
    service.seed_raw_input(orch)
    return orch


def judge(run: PipelineRun, basis: str) -> None:
    """打印运行标识、步骤状态与判定依据，满足测试日志可追溯要求。"""
    steps = ", ".join(
        f"{name}={s.status.value}/{s.reason.value}/attempts={s.attempts}"
        for name, s in run.steps.items()
    )
    logger.info(
        "判定 run_id=%s run_status=%s 步骤[%s] 依据=%s",
        run.run_id, run.status.value, steps, basis,
    )


def assert_step(run: PipelineRun, step: str, status: StepStatus,
                reason: FailureReason | None = None) -> None:
    state = run.steps[step]
    assert state.status == status, f"{run.run_id} 步骤 {step} 期望 {status} 实际 {state.status}"
    if reason is not None:
        assert state.reason == reason, (
            f"{run.run_id} 步骤 {step} 失败原因期望 {reason} 实际 {state.reason}"
        )


def transitions_for(run: PipelineRun, step: str) -> list[tuple[str, str]]:
    return [(e.from_status, e.to_status) for e in run.events if e.step == step]


TERMINAL = {"succeeded", "failed", "cancelled", "timed_out"}


def poll_run(client, run_id: str, timeout: float = 5.0) -> dict:
    """通过 HTTP 轮询运行直到进入终态，避免与后台执行线程竞态。"""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        detail = client.get(f"/api/pipelines/runs/{run_id}").json()
        if detail["status"] in TERMINAL:
            return detail
        time.sleep(0.02)
    raise AssertionError(f"运行 {run_id} 在 {timeout}s 内未进入终态")

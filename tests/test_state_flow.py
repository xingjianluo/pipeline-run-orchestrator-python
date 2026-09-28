"""状态流转：多步骤按依赖顺序推进，失败停在正确位置，事件可归属到运行/步骤。"""
from __future__ import annotations

from pipeline_orchestrator.models import FailureReason, RunStatus, StepStatus
from pipeline_orchestrator import service

from .helpers import (
    assert_step,
    fresh_orchestrator,
    judge,
    transitions_for,
)


def test_steps_run_in_dependency_order():
    orch = fresh_orchestrator()
    rid = orch.trigger("sample_pipeline", trigger="test-order")
    run = orch.wait(rid, 5)

    assert run.status == RunStatus.SUCCEEDED
    # 事件按序号单调递增；每个步骤的 RUNNING 都晚于其上游的 SUCCEEDED
    seqs = [(e.step, e.to_status, e.seq) for e in run.events]
    seqs.sort(key=lambda x: x[2])
    finished = {}
    for step, to_status, _seq in seqs:
        if to_status == StepStatus.SUCCEEDED.value:
            finished[step] = _seq
    assert finished["extract"] < finished["transform"] < finished["load"]

    for name in run.steps:
        assert transitions_for(run, name) == [
            ("pending", "running"),
            ("running", "succeeded"),
        ]
    # 每条事件都带运行标识与步骤名
    assert all(e.run_id == rid and e.step for e in run.events)
    judge(run, "全部成功，事件序列满足 extract->transform->load，事件归属正确")


def test_failure_stops_at_failed_step_and_skips_downstream():
    orch = fresh_orchestrator()
    service.set_fail_step("transform")
    rid = orch.trigger("sample_pipeline", trigger="test-fail")
    run = orch.wait(rid, 5)

    assert run.status == RunStatus.FAILED
    assert_step(run, "extract", StepStatus.SUCCEEDED)
    assert_step(run, "transform", StepStatus.FAILED, FailureReason.STEP_ERROR)
    assert_step(run, "load", StepStatus.SKIPPED, FailureReason.UPSTREAM_FAILED)
    # 失败原因可区分：步骤错误 vs 上游失败
    assert "模拟步骤失败" in run.steps["transform"].detail
    assert "transform" in run.steps["load"].detail
    # load 从未进入 running，没有被当成成功
    assert ("running", "skipped") not in transitions_for(run, "load")
    assert ("pending", "skipped") == transitions_for(run, "load")[0]
    judge(run, "transform 失败后停止；load 明确 skipped/upstream_failed 而非成功")

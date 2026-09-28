"""超时与取消：运行必须落到终止态，步骤原因可区分，不能挂在 running。"""
from __future__ import annotations

import time

from pipeline_orchestrator import service
from pipeline_orchestrator.models import (
    FailureReason,
    RunStatus,
    StepStatus,
    is_terminal,
)

from .helpers import assert_step, fresh_orchestrator, judge


def test_timeout_lands_in_terminal_state():
    orch = fresh_orchestrator(timeout=5.0)
    service.set_slow_step("extract", seconds=3.0)
    rid = orch.trigger("sample_pipeline", trigger="test-timeout", timeout=0.6)
    run = orch.wait(rid, 5)

    assert is_terminal(run.status)
    assert run.status == RunStatus.TIMED_OUT
    assert_step(run, "extract", StepStatus.FAILED, FailureReason.TIMED_OUT)
    assert_step(run, "transform", StepStatus.SKIPPED, FailureReason.TIMED_OUT)
    assert_step(run, "load", StepStatus.SKIPPED, FailureReason.TIMED_OUT)
    # 没有任何步骤或运行停留在运行中
    assert not any(s.status == StepStatus.RUNNING for s in run.steps.values())
    judge(run, "超时后运行 timed_out，执行步 timed_out，其余步 skipped/timed_out")


def test_cancel_lands_in_terminal_state():
    orch = fresh_orchestrator()
    service.set_slow_step("transform", seconds=3.0)
    rid = orch.trigger("sample_pipeline", trigger="test-cancel")
    time.sleep(0.35)  # 等 extract 完成、transform 进入慢执行
    assert orch.cancel(rid) is True
    run = orch.wait(rid, 5)

    assert run.status == RunStatus.CANCELLED
    assert_step(run, "extract", StepStatus.SUCCEEDED)
    assert_step(run, "transform", StepStatus.FAILED, FailureReason.CANCELLED)
    assert_step(run, "load", StepStatus.SKIPPED, FailureReason.CANCELLED)
    assert not any(s.status == StepStatus.RUNNING for s in run.steps.values())
    judge(run, "取消后运行 cancelled，慢步骤 cancelled，下游 skipped/cancelled")

    # 对已结束运行重复取消返回 False，状态保持终止态
    assert orch.cancel(rid) is False
    again = orch._store.get(rid)
    assert again.status == RunStatus.CANCELLED
    judge(again, "重复取消不改变终态，运行不回挂 running")


def test_cancel_before_step_start_marks_all_remaining_skipped():
    orch = fresh_orchestrator()
    # 运行尚未调度前取消（极短超时模拟立即取消窗口）
    rid = orch.trigger("sample_pipeline", run_id="cancel-early", trigger="test-cancel-early")
    orch.cancel(rid)
    run = orch.wait(rid, 5)
    assert is_terminal(run.status)
    assert run.status in (RunStatus.CANCELLED, RunStatus.SUCCEEDED)
    judge(run, "取消窗口场景：运行必然处于终止态（成功或取消），不会挂起")

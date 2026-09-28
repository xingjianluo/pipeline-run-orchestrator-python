"""失败处理与重试：原因可区分，重试不重复执行已成功步骤，原运行记录冻结。"""
from __future__ import annotations

from pipeline_orchestrator import service
from pipeline_orchestrator.models import FailureReason, RunStatus, StepStatus
from pipeline_orchestrator.store import FrozenRunError

from .helpers import assert_step, fresh_orchestrator, judge, transitions_for


def test_retry_reuses_succeeded_steps():
    orch = fresh_orchestrator()

    # 第一次：transform 失败，extract 已成功
    service.set_fail_step("transform")
    rid1 = orch.trigger("sample_pipeline", trigger="test-retry-1")
    run1 = orch.wait(rid1, 5)
    assert run1.status == RunStatus.FAILED
    extract_version = run1.steps["extract"].output_version
    assert extract_version
    judge(run1, "首次运行 transform 失败，extract 已产出版本 " + extract_version)

    # 清除故障后重试
    service.set_fail_step(None)
    rid2 = orch.retry(rid1, trigger="test-retry-2")
    run2 = orch.wait(rid2, 5)

    assert run2.status == RunStatus.SUCCEEDED
    # extract 直接复用：attempts=0，没有 running 事件，产物版本与首次一致
    assert run2.steps["extract"].attempts == 0
    assert run2.steps["extract"].output_version == extract_version
    assert transitions_for(run2, "extract") == [("pending", "succeeded")]
    reuse = [e for e in run2.events if "复用" in e.detail]
    assert len(reuse) == 1 and reuse[0].step == "extract"
    # transform/load 重新执行
    assert run2.steps["transform"].attempts == 1
    assert run2.steps["load"].attempts == 1
    # 重试血缘可追溯到来源
    assert run2.trigger == f"test-retry-2:{rid1}"
    judge(run2, "重试复用 extract 产物不重跑，transform/load 重新执行至成功")

    # 原始失败运行记录冻结，不被改写
    frozen = orch._store.get(rid1)
    assert frozen.status == RunStatus.FAILED
    try:
        orch._store.save(frozen)
        raise AssertionError("终态运行应被冻结")
    except FrozenRunError:
        pass
    judge(run1, "原失败运行记录保持 failed 且写入被拒绝（不可改写）")


def test_retry_failure_reasons_distinguishable():
    orch = fresh_orchestrator()
    # extract 失败：transform/load 都是上游原因链
    service.set_fail_step("extract")
    rid = orch.trigger("sample_pipeline", trigger="test-reasons")
    run = orch.wait(rid, 5)

    assert_step(run, "extract", StepStatus.FAILED, FailureReason.STEP_ERROR)
    assert_step(run, "transform", StepStatus.SKIPPED, FailureReason.UPSTREAM_FAILED)
    # load 的上游 transform 是 skipped -> 原因为 upstream_skipped，区别于直接失败
    assert_step(run, "load", StepStatus.SKIPPED, FailureReason.UPSTREAM_SKIPPED)
    reasons = {name: run.steps[name].reason.value for name in run.steps}
    judge(run, f"三类原因可区分 step_error/upstream_failed/upstream_skipped: {reasons}")

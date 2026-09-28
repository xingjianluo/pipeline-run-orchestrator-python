"""失败处理与重试：失败原因可区分、成功步骤不重跑、上游失败导致下游明确跳过。"""
from pipeline_orchestrator.models import FailureReason, RunStatus, StepStatus


def test_step_failure_marks_downstream_skipped_with_distinguishable_reason(env):
    store, ds, orch = env(fail_step="transform", fail_times=10)
    run_id = orch.trigger("demo", {"raw": "demo/raw"})
    run = orch.wait(run_id, 5)

    assert run.status is RunStatus.FAILED
    assert run.failure_reason is FailureReason.STEP_EXCEPTION
    assert "bad value in row" in run.steps["transform"].error_message
    print(f"run={run_id} step=transform 判定依据: "
          f"status={run.steps['transform'].status.value} "
          f"reason={run.steps['transform'].failure_reason.value}")

    # 独立分支 enrich 只依赖 ingest，仍然成功
    assert run.steps["ingest"].status is StepStatus.SUCCEEDED
    assert run.steps["enrich"].status is StepStatus.SUCCEEDED
    # 依赖失败步骤的 aggregate 必须是 skipped，而不是 succeeded/pending
    agg = run.steps["aggregate"]
    assert agg.status is StepStatus.SKIPPED
    assert agg.failure_reason is FailureReason.UPSTREAM_FAILED
    print(f"run={run_id} step=aggregate 判定依据: 跳过原因={agg.failure_reason.value}")

    lineage = store.list_lineage(run_id)[0]
    assert lineage.status == "failed"
    skipped_outputs = {o["step"]: o for o in lineage.outputs}
    assert skipped_outputs["aggregate"]["status"] == "skipped"
    assert skipped_outputs["aggregate"]["failure_reason"] == "upstream_failed"


def test_retry_does_not_rerun_succeeded_steps(env):
    store, ds, orch = env(fail_step="transform", fail_times=1)

    first_id = orch.trigger("demo", {"raw": "demo/raw"})
    first = orch.wait(first_id, 5)
    assert first.status is RunStatus.FAILED

    second_id = orch.retry(first_id)
    second = orch.wait(second_id, 5)
    print(f"判定依据: 父运行={first_id} 重试运行={second_id}")

    assert second.status is RunStatus.SUCCEEDED
    assert second.parent_run_id == first_id
    assert second.trigger == f"retry-of:{first_id}"

    # ingest/enrich 在父运行已成功：重试时复用同一产出版本，未重新执行
    for name in ("ingest", "enrich"):
        assert second.steps[name].status is StepStatus.SUCCEEDED
        assert second.steps[name].output_version == first.steps[name].output_version
        print(f"run={second_id} step={name} 判定依据: 复用父运行产出版本 "
              f"{second.steps[name].output_version[:8]}，未重复执行")

    # transform/aggregate 在父运行未成功，重试中被真正执行
    assert second.steps["transform"].output_version
    assert second.steps["aggregate"].output_version
    # 父运行记录保持失败、未被改写
    assert store.get_run(first_id).status is RunStatus.FAILED
    assert store.list_lineage(first_id)[0].status == "failed"
    assert store.list_lineage(second_id)[0].status == "succeeded"


def test_retry_of_upstream_failure_skips_chain_consistently(env):
    """源步骤持续失败时：所有下游两次运行中都保持 skipped，血缘一致。"""
    store, ds, orch = env(fail_step="ingest", fail_times=10)
    first_id = orch.trigger("demo", {"raw": "demo/raw"})
    first = orch.wait(first_id, 5)
    assert first.steps["ingest"].failure_reason is FailureReason.STEP_EXCEPTION
    for name in ("transform", "enrich", "aggregate"):
        assert first.steps[name].status is StepStatus.SKIPPED
        assert first.steps[name].failure_reason is FailureReason.UPSTREAM_FAILED

    second_id = orch.retry(first_id)
    second = orch.wait(second_id, 5)
    print(f"run={second_id} 判定依据: 源步骤仍失败，下游状态="
          f"{ {n: second.steps[n].status.value for n in second.steps} }")
    assert second.status is RunStatus.FAILED
    for name in ("transform", "enrich", "aggregate"):
        assert second.steps[name].status is StepStatus.SKIPPED

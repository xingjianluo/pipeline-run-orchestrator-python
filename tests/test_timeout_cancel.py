"""超时与取消：运行必须落到终止态，不能一直挂在 running；下游明确跳过。"""
import time


from pipeline_orchestrator.datasource import LocalDataSource
from pipeline_orchestrator.engine import Orchestrator
from pipeline_orchestrator.executor import StepExecutor
from pipeline_orchestrator.models import FailureReason, RunStatus, StepStatus
from pipeline_orchestrator.pipelines import register_demo
from pipeline_orchestrator.store import LocalStore


def _build(slow_step=None, slow_seconds=10, step_timeouts=None):
    store = LocalStore()
    ds = LocalDataSource()
    ds.put("demo/raw", [1, 2], producer="seed")
    executor = StepExecutor(4)
    orch = Orchestrator(store=store, data_source=ds, executor=executor)
    register_demo(orch, ds, slow_step=slow_step, slow_seconds=slow_seconds)
    for name, seconds in (step_timeouts or {}).items():
        store.get_pipeline("demo").step(name).timeout_seconds = seconds
    return store, ds, orch, executor


def test_run_timeout_reaches_terminal_state(env):
    store, ds, orch = env(slow_step="ingest", slow_seconds=10)
    run_id = orch.trigger("demo", {"raw": "demo/raw"}, run_timeout_seconds=0.4)
    run = orch.wait(run_id, timeout=5)

    print(f"run={run_id} step=- 判定依据: 运行级超时，状态={run.status.value} "
          f"原因={run.failure_reason.value}")
    assert run.status is RunStatus.TIMED_OUT
    assert run.failure_reason is FailureReason.RUN_TIMEOUT
    assert run.is_terminal()
    assert run.finished_at
    assert run.steps["ingest"].status is StepStatus.TIMED_OUT
    for name in ("transform", "enrich", "aggregate"):
        assert run.steps[name].status is StepStatus.SKIPPED
    assert store.run_history(run_id)[-1]["status"] == "timed_out"


def test_step_timeout_fails_run_with_step_timeout_reason():
    store, ds, orch, executor = _build(
        slow_step="transform", slow_seconds=10,
        step_timeouts={"transform": 0.3},
    )
    try:
        run_id = orch.trigger("demo", {"raw": "demo/raw"})
        run = orch.wait(run_id, 5)
        print(f"run={run_id} step=transform 判定依据: 步骤超时 "
              f"reason={run.steps['transform'].failure_reason.value}")
        assert run.status is RunStatus.FAILED
        assert run.steps["transform"].status is StepStatus.TIMED_OUT
        assert run.steps["transform"].failure_reason is FailureReason.STEP_TIMEOUT
        assert run.steps["aggregate"].status is StepStatus.SKIPPED
        assert run.steps["aggregate"].failure_reason is FailureReason.UPSTREAM_FAILED
    finally:
        executor.shutdown()


def test_cancel_reaches_terminal_state(env):
    store, ds, orch = env(slow_step="ingest", slow_seconds=10)
    run_id = orch.trigger("demo", {"raw": "demo/raw"})

    time.sleep(0.25)
    assert orch.cancel(run_id) is True
    run = orch.wait(run_id, timeout=5)
    print(f"run={run_id} step=- 判定依据: 用户取消，状态={run.status.value}")
    assert run.status is RunStatus.CANCELLED
    assert run.failure_reason is FailureReason.CANCELLED
    assert run.is_terminal()
    assert run.steps["ingest"].status is StepStatus.CANCELLED
    for name in ("transform", "enrich", "aggregate"):
        assert run.steps[name].status is StepStatus.SKIPPED
    assert store.run_history(run_id)[-1]["status"] == "cancelled"


def test_cancel_is_idempotent_and_unknown_run_returns_false(env):
    store, ds, orch = env(slow_step="ingest", slow_seconds=10)
    run_id = orch.trigger("demo", {"raw": "demo/raw"})
    assert orch.cancel(run_id) is True
    assert orch.cancel(run_id) is True  # 重复取消不报错
    run = orch.wait(run_id, 5)
    assert run.status is RunStatus.CANCELLED
    assert orch.cancel("run_does_not_exist") is False

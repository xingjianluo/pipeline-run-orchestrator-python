"""并发触发：同 run_id / 幂等键的重复或并发触发合并，每步只执行一次。"""
from __future__ import annotations

import threading

from pipeline_orchestrator.models import RunStatus

from .helpers import fresh_orchestrator, judge


def _gather(n: int, fn):
    barrier = threading.Barrier(n)
    results: list[str] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        value = fn()
        with lock:
            results.append(value)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_concurrent_triggers_same_run_id_coalesce():
    orch = fresh_orchestrator()
    ids = _gather(
        8, lambda: orch.trigger("sample_pipeline", run_id="concurrent-run", trigger="concurrent")
    )
    assert set(ids) == {"concurrent-run"}, ids
    run = orch.wait("concurrent-run", 5)
    assert run.status == RunStatus.SUCCEEDED
    # 状态未被互相覆盖：每个步骤恰好一次执行、一次成功
    for name, step in run.steps.items():
        assert step.attempts == 1, (name, step.attempts)
    judge(run, "8 线程同 run_id 并发触发合并为一个运行，每步 attempts=1")


def test_concurrent_triggers_same_idem_key_coalesce():
    orch = fresh_orchestrator()
    ids = _gather(
        6, lambda: orch.trigger("sample_pipeline", idem_key="idem-42", trigger="concurrent")
    )
    unique = set(ids)
    assert len(unique) == 1, ids
    run = orch.wait(unique.pop(), 5)
    assert run.status == RunStatus.SUCCEEDED
    judge(run, "6 线程同幂等键并发触发合并为一个运行")

    # 运行结束后再用同一幂等键触发，仍返回原运行，不产生重复执行
    late_id = orch.trigger("sample_pipeline", idem_key="idem-42", trigger="late")
    assert late_id == ids[0]
    run_after = orch._store.get(late_id)
    assert all(s.attempts == 1 for s in run_after.steps.values())
    judge(run_after, "结束后同幂等键重复触发仍返回原运行，没有步骤被跑第二遍")


def test_distinct_triggers_run_independently():
    orch = fresh_orchestrator()
    ids = _gather(
        5, lambda: orch.trigger("sample_pipeline", trigger="parallel-distinct")
    )
    assert len(set(ids)) == 5, ids  # 无幂等约束时是 5 个独立运行
    for rid in ids:
        run = orch.wait(rid, 5)
        assert run.status == RunStatus.SUCCEEDED
        assert {s.status.value for s in run.steps.values()} == {"succeeded"}
    judge(orch._store.get(ids[0]), "5 个独立触发各自成功，运行标识互不相同且互不覆盖")

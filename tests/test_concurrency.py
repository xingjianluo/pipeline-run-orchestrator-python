"""并发触发：相同输入合并、不同输入互不覆盖、步骤不重复执行。"""
import threading

from pipeline_orchestrator.models import RunStatus


def test_concurrent_identical_triggers_coalesce(env):
    store, ds, orch = env(slow_step="transform", slow_seconds=0.4)

    ids = []
    barrier = threading.Barrier(5)

    def fire():
        barrier.wait()  # 尽量让 5 个线程同时进入 trigger
        ids.append(orch.trigger("demo", {"raw": "demo/raw"}))

    threads = [threading.Thread(target=fire) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"判定依据: 5 个并发触发返回 run_id={ids}")
    assert len(set(ids)) == 1, "相同输入版本的并发触发必须合并为同一个运行"

    run_id = ids[0]
    run = orch.wait(run_id, timeout=5)
    assert run.status is RunStatus.SUCCEEDED

    # 每个步骤在历史快照中只出现一次 running（没有被重复执行）
    for step_name in run.steps:
        running_snapshots = [
            snap for snap in store.run_history(run_id)
            if snap["steps"][step_name]["status"] == "running"
        ]
        print(f"run={run_id} step={step_name} 判定依据: "
              f"running 快照数={len(running_snapshots)}")
        assert len(running_snapshots) == 1


def test_different_input_versions_run_independently(env):
    store, ds, orch = env()
    v1 = ds.current_version("demo/raw")
    rid1 = orch.trigger("demo", {"raw": f"demo/raw@{v1}"})
    orch.wait(rid1, 5)

    ds.put("demo/raw", [7, 8], producer="test-seed")
    v2 = ds.current_version("demo/raw")
    rid2 = orch.trigger("demo", {"raw": f"demo/raw@{v2}"})
    orch.wait(rid2, 5)

    print(f"判定依据: run1={rid1}@{v1[:8]} run2={rid2}@{v2[:8]}")
    assert rid1 != rid2
    runs = store.list_runs("demo")
    assert len(runs) == 2
    lin1 = store.list_lineage(rid1)[0]
    lin2 = store.list_lineage(rid2)[0]
    assert lin1.input_versions != lin2.input_versions
    # 两条运行的血缘记录都完整保留、互不覆盖
    assert all(r.status is RunStatus.SUCCEEDED for r in runs)


def test_parallel_runs_of_different_inputs_do_not_clobber(env):
    store, ds, orch = env()
    ds.put("raw-a", [1, 2], producer="seed-a")
    ds.put("raw-b", [10, 20], producer="seed-b")

    # 两个不同输入引用，各自固定版本，并发触发
    rid_a = orch.trigger("demo", {"raw": "raw-a"})
    rid_b = orch.trigger("demo", {"raw": "raw-b"})
    ra = orch.wait(rid_a, 5)
    rb = orch.wait(rid_b, 5)
    print(f"判定依据: runA={rid_a} runB={rid_b} 状态分别为 "
          f"{ra.status.value}/{rb.status.value}")
    assert ra.status is RunStatus.SUCCEEDED and rb.status is RunStatus.SUCCEEDED
    # A 的输入版本没有出现在 B 的血缘里
    assert store.list_lineage(rid_a)[0].input_versions != \
        store.list_lineage(rid_b)[0].input_versions

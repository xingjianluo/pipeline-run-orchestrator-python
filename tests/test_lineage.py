"""血缘与版本：相同输入一致结论；回滚/覆盖后历史运行记录不被改写。"""
from pipeline_orchestrator.models import RunStatus


def test_lineage_records_versions_trigger_and_outputs(env):
    store, ds, orch = env()
    input_version = ds.current_version("demo/raw")

    run_id = orch.trigger(
        "demo", {"raw": f"demo/raw@{input_version}"}, trigger="api-call",
    )
    run = orch.wait(run_id, 5)
    assert run.status is RunStatus.SUCCEEDED

    lineage = store.list_lineage(run_id)[0]
    print(f"run={run_id} 判定依据: trigger={lineage.trigger} "
          f"inputs={lineage.input_versions}")
    assert lineage.pipeline_name == "demo"
    assert lineage.pipeline_version == "1.0.0"
    assert lineage.trigger == "api-call"
    assert lineage.input_versions == {"raw": f"demo/raw@{input_version}"}
    succeeded = {o["step"]: o for o in lineage.outputs if o["status"] == "succeeded"}
    assert set(succeeded) == {"ingest", "transform", "enrich", "aggregate"}
    assert all(o["ref"].startswith("outputs/demo/") and o["version"]
               for o in succeeded.values())


def test_same_input_version_rerun_gives_same_conclusion(env):
    store, ds, orch = env()
    pinned = f"demo/raw@{ds.current_version('demo/raw')}"

    rid1 = orch.trigger("demo", {"raw": pinned})
    rid2 = orch.trigger("demo", {"raw": pinned})
    r1 = orch.wait(rid1, 5)
    r2 = orch.wait(rid2, 5)

    l1 = store.list_lineage(rid1)[0]
    l2 = store.list_lineage(rid2)[0]
    versions1 = {o["step"]: o["version"] for o in l1.outputs}
    versions2 = {o["step"]: o["version"] for o in l2.outputs}
    print(f"判定依据: run={rid1} 与 run={rid2} 输入版本相同，产出版本集合一致")
    assert r1.status is r2.status is RunStatus.SUCCEEDED
    assert versions1 == versions2  # 内容寻址：相同输入+相同代码 -> 相同输出版本


def test_overwrite_and_rollback_do_not_rewrite_finished_run(env):
    store, ds, orch = env()
    v1 = ds.current_version("demo/raw")
    run_id = orch.trigger("demo", {"raw": f"demo/raw@{v1}"})
    orch.wait(run_id, 5)
    original_snapshots = store.run_history(run_id)
    original_lineage = store.list_lineage(run_id)[0]
    original_outputs = {o["step"]: o["version"] for o in original_lineage.outputs}

    # 覆盖输入 -> 新版本；再回滚到旧版本
    ds.put("demo/raw", [9, 9, 9, 9], producer="test-seed")
    v2 = ds.current_version("demo/raw")
    assert v2 != v1
    ds.rollback("demo/raw", v1)
    assert ds.current_version("demo/raw") == v1
    print(f"判定依据: 输入经历 v1={v1[:8]} -> v2={v2[:8]} -> 回滚 v1")

    # 已完成运行的状态、历史快照、血缘都不得被改写
    again = store.get_run(run_id)
    assert again.status is RunStatus.SUCCEEDED
    assert again.input_versions == {"raw": f"demo/raw@{v1}"}
    assert store.run_history(run_id) == original_snapshots
    lineage_after = store.list_lineage(run_id)[0]
    after_outputs = {o["step"]: o["version"] for o in lineage_after.outputs}
    assert after_outputs == original_outputs

    # 用新输入版本运行得到独立结论，且历史版本 v2 仍可读取
    rid_new = orch.trigger("demo", {"raw": f"demo/raw@{v2}"})
    r_new = orch.wait(rid_new, 5)
    assert r_new.status is RunStatus.SUCCEEDED
    assert store.list_lineage(run_id)[0].outputs != \
        store.list_lineage(rid_new)[0].outputs
    print(f"run={rid_new} 判定依据: 新版本输入独立产出，旧 run={run_id} 不变")


def test_retry_lineage_links_parent(env):
    store, ds, orch = env(fail_step="aggregate", fail_times=1)
    parent_id = orch.trigger("demo", {"raw": "demo/raw"}, trigger="manual")
    parent = orch.wait(parent_id, 5)
    assert parent.status is RunStatus.FAILED

    child_id = orch.retry(parent_id)
    child = orch.wait(child_id, 5)
    assert child.status is RunStatus.SUCCEEDED
    child_lineage = store.list_lineage(child_id)[0]
    assert child_lineage.parent_run_id == parent_id
    assert child_lineage.trigger == f"retry-of:{parent_id}"
    print(f"run={child_id} step=- 判定依据: 血缘父运行={parent_id}")

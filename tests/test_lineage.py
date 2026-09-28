"""血缘与版本追溯：版本可查、相同输入结论一致、覆盖/回滚不改写历史。"""
from __future__ import annotations

from pipeline_orchestrator.models import RunStatus

from .helpers import fresh_orchestrator, judge


def test_lineage_records_inputs_outputs_and_trigger():
    orch = fresh_orchestrator()
    rid = orch.trigger("sample_pipeline", trigger="lineage-probe")
    run = orch.wait(rid, 5)
    lineage = orch.lineage(rid)

    assert lineage["run_id"] == rid
    assert lineage["trigger"] == "lineage-probe"
    assert lineage["status"] == RunStatus.SUCCEEDED.value
    assert "raw_input" in lineage["input_versions"]
    assert set(lineage["outputs"]) == {"extract", "transform", "load"}
    judge(run, f"血缘记录输入版本/三步产物版本/触发来源: {lineage['input_versions']}")


def test_same_input_version_is_deterministic():
    orch = fresh_orchestrator()
    r1 = orch.wait(orch.trigger("sample_pipeline", trigger="run-a"), 5)
    r2 = orch.wait(orch.trigger("sample_pipeline", trigger="run-b"), 5)
    o1 = orch.lineage(r1.run_id)["outputs"]
    o2 = orch.lineage(r2.run_id)["outputs"]
    assert o1 == o2  # 相同 raw_input 版本 -> 三步产物版本逐一相同
    assert orch.lineage(r1.run_id)["input_versions"] == orch.lineage(r2.run_id)["input_versions"]
    judge(r2, "相同输入版本两次独立运行，产物版本与结论完全一致（可复现）")


def test_input_overwrite_does_not_rewrite_completed_run():
    orch = fresh_orchestrator()
    ds = orch._ds
    r1 = orch.wait(orch.trigger("sample_pipeline", trigger="before-overwrite"), 5)
    snapshot1 = orch.lineage(r1.run_id)

    old_version = ds.current_version("raw_input")
    ds.put("raw_input", {"records": [{"id": i} for i in range(11)]}, cause="overwrite")
    r2 = orch.wait(orch.trigger("sample_pipeline", trigger="after-overwrite"), 5)
    snapshot2 = orch.lineage(r2.run_id)

    # 新输入产生新产物
    assert snapshot2["input_versions"]["raw_input"] != old_version
    assert snapshot2["outputs"]["extract"] != snapshot1["outputs"]["extract"]
    # 已完成的历史运行记录与血缘不被改写
    assert orch.lineage(r1.run_id) == snapshot1
    judge(r1, "输入被覆盖后，历史运行血缘与输入版本保持原值（不可改写）")


def test_rollback_then_rerun_matches_original_conclusion():
    orch = fresh_orchestrator()
    ds = orch._ds
    original_v = ds.current_version("raw_input")
    r1 = orch.wait(orch.trigger("sample_pipeline", trigger="v1"), 5)
    out_v1 = orch.lineage(r1.run_id)["outputs"]

    ds.put("raw_input", {"records": [{"id": i} for i in range(3)]}, cause="overwrite")
    r2 = orch.wait(orch.trigger("sample_pipeline", trigger="v2"), 5)
    out_v2 = orch.lineage(r2.run_id)["outputs"]
    assert out_v2 != out_v1

    # 回滚到旧版本后重跑，结论与最初一致；v2 那次运行仍冻结
    ds.rollback("raw_input", original_v)
    r3 = orch.wait(orch.trigger("sample_pipeline", trigger="rollback-rerun"), 5)
    out_v3 = orch.lineage(r3.run_id)["outputs"]
    assert out_v3 == out_v1
    assert orch.lineage(r2.run_id)["outputs"] == out_v2
    judge(r3, "回滚输入版本后重跑产物与最初一致，且覆盖期运行记录不变")

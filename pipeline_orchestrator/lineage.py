"""血缘与版本追溯：从一次终止态运行构建不可变血缘记录。"""
from __future__ import annotations

from .models import LineageRecord, PipelineRun
from .store import LocalStore


def build_lineage(run: PipelineRun) -> LineageRecord:
    """汇总运行的输入版本、各步骤产出、触发方式、父运行等。"""
    outputs = [
        {
            "step": step.step_name,
            "ref": step.output_ref,
            "version": step.output_version,
            "status": step.status.value,
            "failure_reason": step.failure_reason.value,
        }
        for step in (run.steps[name] for name in sorted(run.steps))
    ]
    return LineageRecord(
        run_id=run.id,
        pipeline_name=run.pipeline_name,
        pipeline_version=run.pipeline_version,
        trigger=run.trigger,
        parent_run_id=run.parent_run_id,
        status=run.status.value,
        input_versions=dict(run.input_versions),
        outputs=outputs,
        created_at=run.created_at,
        finished_at=run.finished_at,
    )


def record_lineage(store: LocalStore, run: PipelineRun) -> LineageRecord:
    """构建并追加血缘记录（append-only，历史运行记录不被改写）。"""
    record = build_lineage(run)
    store.append_lineage(record)
    return record

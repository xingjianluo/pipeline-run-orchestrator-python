"""HTTP API：触发/查询/重试/取消管道运行，查看状态流转历史与血缘。"""
from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .engine import Orchestrator
from .models import PipelineRun


class TriggerRequest(BaseModel):
    pipeline: str = "demo"
    # 形如 {"raw": "demo/raw"} 或固定版本 {"raw": "demo/raw@<version>"}
    inputs: dict[str, str] | None = None
    trigger: str = "manual"
    run_timeout_seconds: float | None = None


class RetryRequest(BaseModel):
    pass


def run_to_dict(run: PipelineRun) -> dict:
    data = asdict(run)
    data["status"] = run.status.value
    data["failure_reason"] = run.failure_reason.value
    for step in data["steps"].values():
        step["status"] = run.steps[step["step_name"]].status.value
        step["failure_reason"] = run.steps[step["step_name"]].failure_reason.value
    data["terminal"] = run.is_terminal()
    return data


def build_router(orchestrator: Orchestrator) -> APIRouter:
    router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])

    @router.post("/trigger")
    def trigger(req: TriggerRequest) -> dict:
        try:
            run_id = orchestrator.trigger(
                req.pipeline,
                inputs=req.inputs,
                trigger=req.trigger,
                run_timeout_seconds=req.run_timeout_seconds,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"run_id": run_id}

    @router.get("/runs")
    def list_runs(pipeline: str | None = None) -> dict:
        runs = orchestrator.store.list_runs(pipeline)
        return {"runs": [{"run_id": r.id, "pipeline": r.pipeline_name,
                          "version": r.pipeline_version, "status": r.status.value,
                          "trigger": r.trigger, "parent_run_id": r.parent_run_id}
                         for r in runs]}

    @router.get("/runs/{run_id}")
    def get_run(run_id: str) -> dict:
        run = orchestrator.store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"unknown run {run_id}")
        return run_to_dict(run)

    @router.get("/runs/{run_id}/history")
    def run_history(run_id: str) -> dict:
        if orchestrator.store.get_run(run_id) is None:
            raise HTTPException(status_code=404, detail=f"unknown run {run_id}")
        return {"run_id": run_id, "transitions": orchestrator.store.run_history(run_id)}

    @router.post("/runs/{run_id}/retry")
    def retry_run(run_id: str) -> dict:
        try:
            new_run_id = orchestrator.retry(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"run_id": new_run_id, "parent_run_id": run_id}

    @router.post("/runs/{run_id}/cancel")
    def cancel_run(run_id: str) -> dict:
        ok = orchestrator.cancel(run_id)
        if not ok:
            raise HTTPException(status_code=404, detail=f"unknown or finished run {run_id}")
        return {"cancelling": run_id}

    @router.get("/lineage")
    def list_lineage(run_id: str | None = None) -> dict:
        records = orchestrator.store.list_lineage(run_id)
        return {"lineage": [asdict(r) for r in records]}

    @router.get("/runs/{run_id}/lineage")
    def run_lineage(run_id: str) -> dict:
        records = orchestrator.store.list_lineage(run_id)
        if not records:
            raise HTTPException(status_code=404,
                                detail=f"no lineage for run {run_id} (not finished?)")
        return asdict(records[0])

    return router

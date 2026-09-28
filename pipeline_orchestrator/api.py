"""FastAPI 路由：触发/重试/取消/查询运行，与现有服务一起启动。

所有数据都在进程内存中模拟，启动即可用，无需任何外部服务。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from . import service
from .models import run_to_dict
from .service import get_orchestrator

router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])


class TriggerRequest(BaseModel):
    trigger: str = "manual"
    run_id: str | None = None
    idem_key: str | None = None
    timeout: float | None = Field(default=None, gt=0)


class RetryRequest(BaseModel):
    trigger: str = "retry"


class ScenarioRequest(BaseModel):
    """本地故障注入：让某步骤失败或变慢（慢用于验证超时/取消）。"""

    fail_step: str | None = None
    slow_step: str | None = None
    slow_seconds: float = 2.0


class SeedRequest(BaseModel):
    n_records: int = 5


@router.post("/seed")
def seed_raw(req: SeedRequest) -> dict[str, Any]:
    orch = get_orchestrator()
    version = service.seed_raw_input(
        orch, [{"id": i} for i in range(req.n_records)]
    )
    return {"dataset": service.RAW_INPUT, "version": version}


@router.get("")
def list_pipelines() -> dict[str, Any]:
    orch = get_orchestrator()
    return {
        "pipelines": [
            {
                "name": p.name,
                "steps": [
                    {"name": s.name, "upstream": list(s.upstream)} for s in p.steps
                ],
            }
            for p in orch._pipelines.values()  # noqa: SLF001
        ]
    }


@router.post("/{pipeline_name}/trigger")
def trigger_pipeline(pipeline_name: str, req: TriggerRequest) -> dict[str, Any]:
    orch = get_orchestrator()
    try:
        run_id = orch.trigger(
            pipeline_name,
            trigger=req.trigger,
            run_id=req.run_id,
            idem_key=req.idem_key,
            timeout=req.timeout,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"run_id": run_id}


@router.post("/runs/{run_id}/retry")
def retry_run(run_id: str, req: RetryRequest) -> dict[str, Any]:
    orch = get_orchestrator()
    try:
        new_id = orch.retry(run_id, trigger=req.trigger)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"run_id": new_id, "retried_from": run_id}


@router.post("/runs/{run_id}/cancel")
def cancel_run(run_id: str) -> dict[str, Any]:
    orch = get_orchestrator()
    cancelled = orch.cancel(run_id)
    if not cancelled:
        raise HTTPException(status_code=409, detail="运行不存在或已结束，无法取消")
    return {"run_id": run_id, "cancelled": True}


@router.get("/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    orch = get_orchestrator()
    run = orch._store.get(run_id)  # noqa: SLF001
    if run is None:
        raise HTTPException(status_code=404, detail=f"找不到运行 {run_id}")
    return run_to_dict(run)


@router.get("/runs/{run_id}/lineage")
def get_lineage(run_id: str) -> dict[str, Any]:
    orch = get_orchestrator()
    try:
        return orch.lineage(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs")
def list_runs(pipeline: str | None = None) -> dict[str, Any]:
    orch = get_orchestrator()
    runs = orch._store.list_runs(pipeline)  # noqa: SLF001
    return {"runs": [run_to_dict(r) for r in runs]}


@router.post("/scenarios")
def configure_scenario(req: ScenarioRequest) -> dict[str, Any]:
    """注入失败/慢步骤，仅用于本地验证重试、超时、取消。"""
    service.set_fail_step(req.fail_step)
    service.set_slow_step(req.slow_step, req.slow_seconds)
    return {
        "fail_step": req.fail_step,
        "slow_step": req.slow_step,
        "slow_seconds": req.slow_seconds if req.slow_step else 0.0,
    }


@router.post("/scenarios/reset")
def reset_scenario() -> dict[str, Any]:
    service.reset_scenarios()
    return {"reset": True}

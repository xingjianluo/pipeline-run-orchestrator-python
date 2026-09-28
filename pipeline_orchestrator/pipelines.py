"""内置演示管道：上游 ingest 产出数据，transform/enrich 处理，aggregate 汇总。

DAG:
    ingest (源步骤，消费输入 raw)
      -> transform -> enrich
      -> aggregate(transform, enrich 汇聚)

处理函数只读写 LocalDataSource；通过参数注入失败次数 / 延迟，
用于本地验证重试、上游失败跳过、步骤超时、运行取消等场景。
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import Any

from .datasource import LocalDataSource
from .engine import Orchestrator
from .models import PipelineDef, StepDef

PIPELINE_NAME = "demo"


class _FailController:
    """记录某个步骤还剩多少次"应失败"，跨重试计数（默认按管道实例独立）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._remaining: dict[str, int] = defaultdict(int)

    def arm(self, step: str, times: int) -> None:
        with self._lock:
            self._remaining[step] = max(0, times)

    def should_fail(self, step: str) -> bool:
        with self._lock:
            if self._remaining[step] > 0:
                self._remaining[step] -= 1
                return True
            return False


def _make_handlers(fail: _FailController, slow_step: str | None,
                   slow_seconds: float):
    def ingest(ctx: dict[str, Any]) -> dict[str, Any]:
        _maybe_sleep(ctx, "ingest", slow_step, slow_seconds)
        if fail.should_fail("ingest"):
            raise RuntimeError("simulated ingest failure: upstream file corrupt")
        raw = ctx["inputs"].get("raw", [])
        return {"rows": len(raw), "source": "local-datasource", "raw": raw}

    def transform(ctx: dict[str, Any]) -> dict[str, Any]:
        _maybe_sleep(ctx, "transform", slow_step, slow_seconds)
        if fail.should_fail("transform"):
            raise ValueError("simulated transform failure: bad value in row")
        ingested = ctx["upstream"]["ingest"]
        doubled = [v * 2 for v in ingested["raw"]]
        return {"rows": ingested["rows"], "doubled": doubled}

    def enrich(ctx: dict[str, Any]) -> dict[str, Any]:
        _maybe_sleep(ctx, "enrich", slow_step, slow_seconds)
        if fail.should_fail("enrich"):
            raise RuntimeError("simulated enrich failure: enrichment lookup failed")
        ingested = ctx["upstream"]["ingest"]
        tagged = [{"v": v, "tag": f"n{v}"} for v in ingested["raw"]]
        return {"tagged": tagged}

    def aggregate(ctx: dict[str, Any]) -> dict[str, Any]:
        _maybe_sleep(ctx, "aggregate", slow_step, slow_seconds)
        if fail.should_fail("aggregate"):
            raise RuntimeError("simulated aggregate failure")
        transformed = ctx["upstream"]["transform"]
        enriched = ctx["upstream"]["enrich"]
        total = sum(transformed["doubled"])
        return {
            "total": total,
            "count": transformed["rows"],
            "tags": [item["tag"] for item in enriched["tagged"]],
        }

    return {
        "ingest": ingest,
        "transform": transform,
        "enrich": enrich,
        "aggregate": aggregate,
    }


def _maybe_sleep(ctx: dict[str, Any], step: str, slow_step: str | None,
                 slow_seconds: float) -> None:
    if slow_step != step:
        return
    event = ctx.get("cancel_event")
    # 可响应取消的分段睡眠：取消信号到达时立即抛出，避免取消后仍挂在 running
    deadline = time.monotonic() + slow_seconds
    while time.monotonic() < deadline:
        if event is not None and event.is_set():
            raise RuntimeError(f"{step} interrupted by cancellation")
        time.sleep(min(0.05, deadline - time.monotonic()))


def build_demo_pipeline(
    version: str = "1.0.0",
    fail_step: str | None = None,
    fail_times: int = 1,
    slow_step: str | None = None,
    slow_seconds: float = 1.0,
) -> PipelineDef:
    fail = _FailController()
    if fail_step:
        fail.arm(fail_step, fail_times)
    handlers = _make_handlers(fail, slow_step, slow_seconds)

    def step(name: str, upstream: list[str], timeout: float | None = None) -> StepDef:
        return StepDef(name=name, upstream=upstream, timeout_seconds=timeout,
                       handler=handlers[name])

    return PipelineDef(
        name=PIPELINE_NAME,
        version=version,
        steps=[
            step("ingest", []),
            step("transform", ["ingest"]),
            step("enrich", ["ingest"]),
            step("aggregate", ["transform", "enrich"], timeout=5.0),
        ],
    )


def register_demo(
    orchestrator: Orchestrator,
    data_source: LocalDataSource,
    version: str = "1.0.0",
    fail_step: str | None = None,
    fail_times: int = 1,
    slow_step: str | None = None,
    slow_seconds: float = 1.0,
) -> PipelineDef:
    pipeline = build_demo_pipeline(
        version=version, fail_step=fail_step, fail_times=fail_times,
        slow_step=slow_step, slow_seconds=slow_seconds,
    )
    orchestrator.register_pipeline(pipeline)
    return pipeline

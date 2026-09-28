"""装配本地服务：数据源、执行引擎、示例管道（上游产出 -> 下游消费）。

示例管道 sample_pipeline 依赖关系：
    extract -> transform -> load
extract 读取根数据集 raw_input，产出 extract_out；
transform 消费 extract_out，产出 transform_out；
load 消费 transform_out，产出 load_out。

为了本地验证失败/重试/超时/取消，提供全局“场景开关”：
- set_fail_step: 让某个步骤本次及之后抛错（直到清除），用于制造可重试失败；
- set_slow_step: 让某个步骤 sleep 一段时间，用于制造超时/取消。
所有状态都在进程内存中，无外部服务、无凭据。
"""
from __future__ import annotations

import threading
import time

from .executor import Orchestrator, PipelineDef, RunContext
from .models import StepDef
from .store import RunStore, VersionedDataSource

PIPELINE_NAME = "sample_pipeline"
RAW_INPUT = "raw_input"

_scenarios_lock = threading.Lock()
_fail_steps: set[str] = set()
_slow_steps: dict[str, float] = {}
_attempt_counter: dict[str, int] = {}


def set_fail_step(step: str | None) -> None:
    with _scenarios_lock:
        _fail_steps.clear()
        if step:
            _fail_steps.add(step)


def set_slow_step(step: str | None, seconds: float = 2.0) -> None:
    with _scenarios_lock:
        _slow_steps.clear()
        if step:
            _slow_steps[step] = seconds


def _should_fail(step: str) -> bool:
    with _scenarios_lock:
        return step in _fail_steps


def _slow_seconds(step: str) -> float:
    with _scenarios_lock:
        return _slow_steps.get(step, 0.0)


def reset_scenarios() -> None:
    with _scenarios_lock:
        _fail_steps.clear()
        _slow_steps.clear()
        _attempt_counter.clear()


def _attempt(step: str) -> int:
    with _scenarios_lock:
        _attempt_counter[step] = _attempt_counter.get(step, 0) + 1
        return _attempt_counter[step]


def _make_handler(step: str, transform):
    def handler(inputs: dict[str, object], ctx: RunContext) -> None:
        attempt = _attempt(step)
        ctx.raise_if_cancelled()
        slow = _slow_seconds(step)
        if slow:
            # 分片睡眠，保证取消信号能被及时感知
            deadline = time.monotonic() + slow
            while time.monotonic() < deadline:
                ctx.raise_if_cancelled()
                time.sleep(min(0.05, deadline - time.monotonic()))
        if _should_fail(step):
            raise RuntimeError(f"模拟步骤失败: {step}(attempt={attempt})")
        ctx.raise_if_cancelled()
        payload = transform(inputs, attempt)
        ctx.write_output(step, step, payload)

    return handler


def _extract(inputs: dict, attempt: int) -> dict:
    raw = inputs["raw_input"]
    # 产物只包含确定性内容：相同输入版本 -> 相同产物版本（attempt 不进入内容）
    return {"rows": len(raw.get("records", [])), "raw": raw}


def _transform(inputs: dict, attempt: int) -> dict:
    upstream = inputs["extract"]
    rows = upstream["rows"]
    return {"enriched_rows": rows, "checksum": rows * 7 + 1}


def _load(inputs: dict, attempt: int) -> dict:
    upstream = inputs["transform"]
    return {"loaded": upstream["enriched_rows"], "ok": True}


def build_pipeline() -> PipelineDef:
    return PipelineDef(
        name=PIPELINE_NAME,
        steps=(
            StepDef("extract"),
            StepDef("transform", upstream=("extract",)),
            StepDef("load", upstream=("transform",)),
        ),
        handlers={
            "extract": _make_handler("extract", _extract),
            "transform": _make_handler("transform", _transform),
            "load": _make_handler("load", _load),
        },
    )


def build_orchestrator(
    data_source: VersionedDataSource | None = None,
    run_store: RunStore | None = None,
    default_timeout: float = 5.0,
) -> Orchestrator:
    ds = data_source or VersionedDataSource()
    store = run_store or RunStore()
    orch = Orchestrator(ds, store, default_timeout=default_timeout)
    orch.register_pipeline(build_pipeline())
    return orch


def seed_raw_input(orch: Orchestrator, records: list | None = None) -> str:
    """写入一份本地模拟的上游原始数据，返回其内容版本。"""
    payload = {"records": records if records is not None else [{"id": i} for i in range(5)]}
    # 直接用引擎持有的数据源写入；通过私有 ds 仅为装配便利
    version = orch._ds.put(RAW_INPUT, payload, cause="seed")  # noqa: SLF001
    return version


# 全局单例，供 HTTP API 与本地验证脚本共用
orchestrator: Orchestrator | None = None


def get_orchestrator() -> Orchestrator:
    global orchestrator
    if orchestrator is None:
        orchestrator = build_orchestrator()
        seed_raw_input(orchestrator)
    return orchestrator

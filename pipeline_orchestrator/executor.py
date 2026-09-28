"""管道执行引擎：依赖顺序推进、并发互斥、重试、超时与取消。

并发模型：
- 每次触发产生一个 run_id，运行状态由引擎内部对象 + RunStore 快照双重持有；
  状态变更全部在“每运行一把锁”下完成，并追加一条带 run_id/步骤/原因的事件。
- 同一 run 的调度只在单一线程内推进，步骤 PENDING->RUNNING 是原子领取，
  同一步骤不可能执行两遍。
- 相同 idem_key（或相同 run_id）的并发触发会合并为同一个运行。
- 重试产生新运行并冻结原运行；已成功步骤直接复用产物版本，不再执行。
- 超时/取消通过子线程执行 + 轮询 join 感知，运行必然落到 CANCELLED/TIMED_OUT。
"""
from __future__ import annotations

import itertools
import logging
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .models import (
    FailureReason,
    PipelineRun,
    RunEvent,
    RunStatus,
    StepDef,
    StepState,
    StepStatus,
    is_terminal,
    now_utc,
)
from .store import RunStore, VersionedDataSource

logger = logging.getLogger("pipeline_orchestrator")

StepHandler = Callable[[dict[str, Any], "RunContext"], None]

_TERMINAL_STEP = frozenset(
    {StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED}
)


class PipelineValidationError(ValueError):
    pass


class RunCancelled(Exception):
    """协作式取消：处理函数可在长任务中自行检查 context.cancel_event。"""


@dataclass
class PipelineDef:
    name: str
    steps: tuple[StepDef, ...]
    handlers: dict[str, StepHandler] = field(default_factory=dict)

    def topo_order(self) -> list[StepDef]:
        """Kahn 拓扑排序，并校验依赖存在、无环。"""
        by_name = {s.name: s for s in self.steps}
        if len(by_name) != len(self.steps):
            raise PipelineValidationError(f"管道 {self.name} 存在重名步骤")
        indegree = {s.name: 0 for s in self.steps}
        children: dict[str, list[str]] = {s.name: [] for s in self.steps}
        for step in self.steps:
            for up in step.upstream:
                if up not in by_name:
                    raise PipelineValidationError(
                        f"步骤 {step.name} 依赖了不存在的步骤 {up}"
                    )
                indegree[step.name] += 1
                children[up].append(step.name)
        ready = [s.name for s in self.steps if indegree[s.name] == 0]
        order: list[StepDef] = []
        while ready:
            name = ready.pop(0)
            order.append(by_name[name])
            for child in children[name]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
        if len(order) != len(self.steps):
            raise PipelineValidationError(f"管道 {self.name} 的依赖存在环")
        for name in by_name:
            if name not in self.handlers:
                raise PipelineValidationError(f"步骤 {name} 缺少处理函数")
        return order


class RunContext:
    """传给步骤处理函数的上下文：读取固定版本输入、写出版本化产物。"""

    def __init__(
        self,
        run_id: str,
        data_source: VersionedDataSource,
        input_versions: dict[str, str],
        pinned_inputs: dict[str, str],
        cancel_event: threading.Event,
        rlock: threading.RLock,
    ) -> None:
        self.run_id = run_id
        self._ds = data_source
        self._input_versions = input_versions  # 触发时固定的版本快照
        self._pinned_inputs = pinned_inputs    # 运行中惰性固定的额外输入
        self.cancel_event = cancel_event
        self._lock = rlock
        self.output_versions: dict[str, str] = {}
        self.output_names: dict[str, str] = {}

    def read_input(self, dataset: str, version: str | None = None) -> Any:
        """按运行开始时固定的版本读取输入；新名字首次读取时固定当前版本。"""
        with self._lock:
            if version is not None:
                pinned_version = version
            else:
                pinned_version = self._input_versions.get(dataset) or self._pinned_inputs.get(dataset)
                if pinned_version is None:
                    pinned_version = self._ds.current_version(dataset)
                    if pinned_version is None:
                        raise KeyError(f"输入数据集 {dataset} 不存在")
                    self._pinned_inputs[dataset] = pinned_version
        payload, pinned_version = self._ds.get(dataset, pinned_version)
        logger.info("run=%s read dataset=%s version=%s", self.run_id, dataset, pinned_version)
        return payload

    def write_output(self, step: str, dataset: str, payload: Any) -> str:
        version = self._ds.put(dataset, payload, cause=f"run={self.run_id} step={step}")
        with self._lock:
            self.output_versions[step] = version
            self.output_names[step] = dataset
        return version

    def raise_if_cancelled(self) -> None:
        if self.cancel_event.is_set():
            raise RunCancelled("运行已被取消")


@dataclass
class _RunHandle:
    run: PipelineRun
    order: list[StepDef]
    lock: threading.RLock
    cancel_event: threading.Event
    done_event: threading.Event
    ctx: RunContext
    deadline: float | None


class Orchestrator:
    def __init__(
        self,
        data_source: VersionedDataSource,
        run_store: RunStore,
        default_timeout: float = 5.0,
    ) -> None:
        self._ds = data_source
        self._store = run_store
        self._default_timeout = default_timeout
        self._lock = threading.RLock()
        self._pipelines: dict[str, PipelineDef] = {}
        self._handles: dict[str, _RunHandle] = {}
        self._idem: dict[tuple[str, str], str] = {}
        self._seq = itertools.count(1)

    # ---------- 管道注册 ----------

    def register_pipeline(self, pipeline: PipelineDef) -> None:
        pipeline.topo_order()  # 启动前校验
        with self._lock:
            self._pipelines[pipeline.name] = pipeline

    # ---------- 触发 ----------

    def trigger(
        self,
        pipeline_name: str,
        trigger: str = "manual",
        input_overrides: dict[str, str] | None = None,
        run_id: str | None = None,
        idem_key: str | None = None,
        timeout: float | None = None,
    ) -> str:
        with self._lock:
            if pipeline_name not in self._pipelines:
                raise KeyError(f"未注册的管道: {pipeline_name}")
            if run_id is not None and run_id in self._handles:
                return run_id  # 相同 run_id 并发/重复触发：合并
            if idem_key is not None:
                # 幂等键永久去重：无论首个运行进行中还是已结束，同键都返回同一运行
                existing = self._idem.get((pipeline_name, idem_key))
                if existing is not None and existing in self._handles:
                    return existing
            new_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
            run = self._build_run(
                self._pipelines[pipeline_name],
                new_id,
                trigger,
                dict(input_overrides or {}),
            )
            return self._start(pipeline_name, run, timeout, idem_key)

    # ---------- 重试 ----------

    def retry(self, run_id: str, trigger: str = "retry") -> str:
        original = self._store.get(run_id)
        if original is None:
            raise KeyError(f"找不到运行 {run_id}")
        if not is_terminal(original.status):
            raise RuntimeError(f"运行 {run_id} 尚未结束，不能重试")
        if original.status == RunStatus.SUCCEEDED:
            raise RuntimeError(f"运行 {run_id} 已成功，无需重试")
        with self._lock:
            pipeline = self._pipelines[original.pipeline_name]
            new_id = f"run-{uuid.uuid4().hex[:12]}"
            run = self._build_run(
                pipeline,
                new_id,
                f"{trigger}:{run_id}",
                dict(original.input_versions),
            )
            # 已成功步骤直接复用，不重新执行；其余步骤回到 PENDING
            for name, step in run.steps.items():
                old = original.steps.get(name)
                if old is not None and old.status == StepStatus.SUCCEEDED:
                    step.status = StepStatus.SUCCEEDED
                    step.output_name = old.output_name
                    step.output_version = old.output_version
                    step.ended_at = now_utc()
            return self._start(pipeline.name, run, None, None, seed_succeeded=True)

    # ---------- 取消 ----------

    def cancel(self, run_id: str) -> bool:
        with self._lock:
            handle = self._handles.get(run_id)
        if handle is None or is_terminal(handle.run.status):
            return False
        handle.cancel_event.set()
        logger.info("run=%s cancel requested", run_id)
        return True

    # ---------- 查询 ----------

    def wait(self, run_id: str, timeout: float | None = None) -> PipelineRun:
        with self._lock:
            handle = self._handles.get(run_id)
        if handle is None:
            found = self._store.get(run_id)
            if found is None:
                raise KeyError(f"找不到运行 {run_id}")
            return found
        handle.done_event.wait(timeout)
        result = self._store.get(run_id)
        assert result is not None
        return result

    def lineage(self, run_id: str) -> dict[str, Any]:
        run = self._store.get(run_id)
        if run is None:
            raise KeyError(f"找不到运行 {run_id}")
        outputs = {
            name: s.output_version
            for name, s in run.steps.items()
            if s.status == StepStatus.SUCCEEDED and s.output_version
        }
        from .models import Lineage

        return Lineage(
            run_id=run.run_id,
            pipeline_name=run.pipeline_name,
            trigger=run.trigger,
            status=run.status.value,
            input_versions=dict(run.input_versions),
            outputs=outputs,
            created_at=run.created_at,
            updated_at=run.updated_at,
        ).to_dict()

    # ---------- 内部实现 ----------

    def _build_run(
        self,
        pipeline: PipelineDef,
        run_id: str,
        trigger: str,
        input_versions: dict[str, str],
    ) -> PipelineRun:
        ts = now_utc()
        return PipelineRun(
            run_id=run_id,
            pipeline_name=pipeline.name,
            trigger=trigger,
            status=RunStatus.PENDING,
            input_versions=input_versions,
            steps={s.name: StepState(name=s.name) for s in pipeline.steps},
            created_at=ts,
            updated_at=ts,
        )

    def _start(
        self,
        pipeline_name: str,
        run: PipelineRun,
        timeout: float | None,
        idem_key: str | None,
        seed_succeeded: bool = False,
    ) -> str:
        pipeline = self._pipelines[pipeline_name]
        order = pipeline.topo_order()
        lock = threading.RLock()
        cancel_event = threading.Event()
        done_event = threading.Event()
        pinned: dict[str, str] = {}
        ctx = RunContext(
            run.run_id, self._ds, run.input_versions, pinned, cancel_event, lock
        )
        seconds = self._default_timeout if timeout is None else timeout
        deadline = (time.monotonic() + seconds) if seconds and seconds > 0 else None
        handle = _RunHandle(
            run=run, order=order, lock=lock, cancel_event=cancel_event,
            done_event=done_event, ctx=ctx, deadline=deadline,
        )
        self._handles[run.run_id] = handle
        if idem_key is not None:
            self._idem[(pipeline_name, idem_key)] = run.run_id
        self._persist(run)
        if seed_succeeded:
            # 复用步骤也要留痕：记录“复用”事件而非执行事件
            with lock:
                for name, step in run.steps.items():
                    if step.status == StepStatus.SUCCEEDED:
                        self._emit(
                            run, name,
                            StepStatus.PENDING.value, StepStatus.SUCCEEDED.value,
                            FailureReason.NONE.value,
                            f"复用上一次运行已成功的产物 {step.output_version}",
                        )
            self._persist(run)
        worker = threading.Thread(
            target=self._run_loop, args=(pipeline, handle), daemon=True,
            name=f"pipeline-{run.run_id}",
        )
        worker.start()
        return run.run_id

    def _persist(self, run: PipelineRun) -> None:
        run.updated_at = now_utc()
        self._store.save(run)

    def _emit(
        self,
        run: PipelineRun,
        step: str,
        from_status: str,
        to_status: str,
        reason: str,
        detail: str = "",
    ) -> None:
        event = RunEvent(
            seq=next(self._seq),
            run_id=run.run_id,
            step=step,
            from_status=from_status,
            to_status=to_status,
            reason=reason,
            at=now_utc(),
            detail=detail,
        )
        run.events.append(event)
        logger.info(
            "run=%s step=%s %s->%s reason=%s %s",
            run.run_id, step, from_status, to_status, reason, detail,
        )

    def _run_loop(self, pipeline: PipelineDef, handle: _RunHandle) -> None:
        run = handle.run
        try:
            with handle.lock:
                run.status = RunStatus.RUNNING
                self._persist(run)
            self._schedule(pipeline, handle)
        except Exception:  # noqa: BLE001 - 兜底，保证运行一定到终态
            logger.error("run=%s scheduler crashed: %s", run.run_id, traceback.format_exc())
            with handle.lock:
                if not is_terminal(run.status):
                    run.status = RunStatus.FAILED
                    for step in run.steps.values():
                        if step.status not in _TERMINAL_STEP:
                            step.status = StepStatus.SKIPPED
                            step.reason = FailureReason.UPSTREAM_FAILED
                    self._persist(run)
        finally:
            handle.done_event.set()

    def _remaining_seconds(self, handle: _RunHandle) -> float | None:
        if handle.deadline is None:
            return None
        return max(0.0, handle.deadline - time.monotonic())

    def _schedule(self, pipeline: PipelineDef, handle: _RunHandle) -> None:
        run = handle.run
        while True:
            # 把运行中新固定的输入版本并入血缘
            with handle.lock:
                for name, version in handle.ctx._pinned_inputs.items():
                    run.input_versions.setdefault(name, version)

            action = self._next_action(handle)
            if action is None:
                break
            kind, step_def = action

            if kind == "skip":
                self._apply_skip(handle, step_def)
                continue

            # 执行前先检查取消/超时
            if handle.cancel_event.is_set():
                self._abort_remaining(handle, FailureReason.CANCELLED, RunStatus.CANCELLED)
                return
            remaining = self._remaining_seconds(handle)
            if remaining is not None and remaining <= 0:
                self._abort_remaining(handle, FailureReason.TIMED_OUT, RunStatus.TIMED_OUT)
                return

            self._execute_step(pipeline, handle, step_def, remaining)

        self._finalize(handle)

    def _next_action(self, handle: _RunHandle) -> tuple[str, StepDef] | None:
        run = handle.run
        with handle.lock:
            for step_def in handle.order:
                step = run.steps[step_def.name]
                if step.status != StepStatus.PENDING:
                    continue
                ups = [run.steps[u] for u in step_def.upstream]
                if any(s.status == StepStatus.PENDING or s.status == StepStatus.RUNNING for s in ups):
                    continue  # 上游未决，等下一轮
                if any(s.status in (StepStatus.FAILED, StepStatus.SKIPPED) for s in ups):
                    return ("skip", step_def)
                return ("run", step_def)
            return None

    def _apply_skip(self, handle: _RunHandle, step_def: StepDef) -> None:
        run = handle.run
        with handle.lock:
            step = run.steps[step_def.name]
            upstream_states = {u: run.steps[u].status for u in step_def.upstream}
            if any(s == StepStatus.FAILED for s in upstream_states.values()):
                reason = FailureReason.UPSTREAM_FAILED
            else:
                reason = FailureReason.UPSTREAM_SKIPPED
            failed_up = [u for u, s in upstream_states.items() if s != StepStatus.SUCCEEDED]
            step.status = StepStatus.SKIPPED
            step.reason = reason
            step.detail = f"上游步骤未成功: {','.join(failed_up)}"
            step.started_at = step.ended_at = now_utc()
            self._emit(
                run, step.name,
                StepStatus.PENDING.value, StepStatus.SKIPPED.value,
                reason.value, step.detail,
            )
            self._persist(run)

    def _execute_step(
        self,
        pipeline: PipelineDef,
        handle: _RunHandle,
        step_def: StepDef,
        remaining: float | None,
    ) -> None:
        run = handle.run
        with handle.lock:
            step = run.steps[step_def.name]
            # 原子领取：只有 PENDING 才能进入 RUNNING，杜绝重复执行
            if step.status != StepStatus.PENDING:
                return
            step.status = StepStatus.RUNNING
            step.attempts += 1
            step.started_at = now_utc()
            self._emit(
                run, step.name,
                StepStatus.PENDING.value, StepStatus.RUNNING.value,
                FailureReason.NONE.value,
            )
            self._persist(run)

        outcome: dict[str, str] = {}

        def work() -> None:
            try:
                handler = pipeline.handlers[step_def.name]
                inputs: dict[str, Any] = {}
                for up in step_def.upstream:
                    up_step = run.steps[up]
                    # 精确读取上游步骤产出的数据集版本（重试复用时也一致）
                    inputs[up] = handle.ctx.read_input(
                        up_step.output_name or up, up_step.output_version
                    )
                if not step_def.upstream:
                    inputs["raw_input"] = handle.ctx.read_input("raw_input")
                handler(inputs, handle.ctx)
                outcome["result"] = "ok"
            except RunCancelled as exc:
                outcome["result"] = "cancelled"
                outcome["detail"] = str(exc)
            except Exception as exc:  # noqa: BLE001 - 失败原因要落库
                outcome["result"] = "error"
                outcome["detail"] = f"{type(exc).__name__}: {exc}"

        thread = threading.Thread(target=work, daemon=True, name=f"step-{run.run_id}-{step_def.name}")
        thread.start()

        # 轮询 join：同时感知超时与取消
        poll = 0.05
        while True:
            thread.join(poll)
            if not thread.is_alive():
                break
            if handle.cancel_event.is_set():
                break
            left = self._remaining_seconds(handle)
            if left is not None and left <= 0:
                break

        with handle.lock:
            step.ended_at = now_utc()
            if outcome.get("result") == "ok":
                step.status = StepStatus.SUCCEEDED
                step.reason = FailureReason.NONE
                step.output_name = handle.ctx.output_names.get(step_def.name, step_def.name)
                step.output_version = handle.ctx.output_versions.get(step_def.name, "")
                self._emit(run, step.name, StepStatus.RUNNING.value,
                           StepStatus.SUCCEEDED.value, FailureReason.NONE.value)
            elif outcome.get("result") == "error":
                step.status = StepStatus.FAILED
                step.reason = FailureReason.STEP_ERROR
                step.detail = outcome["detail"]
                self._emit(run, step.name, StepStatus.RUNNING.value,
                           StepStatus.FAILED.value, FailureReason.STEP_ERROR.value,
                           outcome["detail"])
            elif outcome.get("result") == "cancelled":
                step.status = StepStatus.FAILED
                step.reason = FailureReason.CANCELLED
                step.detail = outcome.get("detail", "运行被取消")
                self._emit(run, step.name, StepStatus.RUNNING.value,
                           StepStatus.FAILED.value, FailureReason.CANCELLED.value,
                           step.detail)
            elif handle.cancel_event.is_set():
                # 线程在取消时仍存活：步骤判为取消
                step.status = StepStatus.FAILED
                step.reason = FailureReason.CANCELLED
                step.detail = "步骤执行中收到取消信号"
                self._emit(run, step.name, StepStatus.RUNNING.value,
                           StepStatus.FAILED.value, FailureReason.CANCELLED.value,
                           step.detail)
            else:
                # 线程在超时时仍存活
                step.status = StepStatus.FAILED
                step.reason = FailureReason.TIMED_OUT
                step.detail = f"步骤执行超过运行截止时间"
                self._emit(run, step.name, StepStatus.RUNNING.value,
                           StepStatus.FAILED.value, FailureReason.TIMED_OUT.value,
                           step.detail)
            self._persist(run)

        # 超时/取消发生时，把其余未决步骤全部落终态
        if step.reason == FailureReason.TIMED_OUT:
            self._abort_remaining(handle, FailureReason.TIMED_OUT, RunStatus.TIMED_OUT)
        elif step.reason == FailureReason.CANCELLED:
            self._abort_remaining(handle, FailureReason.CANCELLED, RunStatus.CANCELLED)

    def _abort_remaining(
        self, handle: _RunHandle, reason: FailureReason, run_status: RunStatus
    ) -> None:
        run = handle.run
        with handle.lock:
            if is_terminal(run.status):
                return
            for step in run.steps.values():
                if step.status in (StepStatus.PENDING, StepStatus.RUNNING):
                    step.status = StepStatus.SKIPPED
                    step.reason = reason
                    step.detail = f"运行{run_status.value}，未执行"
                    step.started_at = step.ended_at = now_utc()
                    self._emit(
                        run, step.name,
                        StepStatus.PENDING.value, StepStatus.SKIPPED.value,
                        reason.value, step.detail,
                    )
            run.status = run_status
            self._persist(run)

    def _finalize(self, handle: _RunHandle) -> None:
        run = handle.run
        with handle.lock:
            if is_terminal(run.status):
                return
            statuses = {s.status for s in run.steps.values()}
            if statuses <= {StepStatus.SUCCEEDED}:
                run.status = RunStatus.SUCCEEDED
            elif StepStatus.FAILED in statuses:
                run.status = RunStatus.FAILED
            else:
                run.status = RunStatus.FAILED  # 只有跳过没有成功路径时
            self._persist(run)

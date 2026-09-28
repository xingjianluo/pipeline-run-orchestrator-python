"""编排引擎：按依赖顺序推进步骤，处理失败、跳过、重试、取消与超时。

关键保证：
- 每个管道一把 RLock，触发串行化；相同 (管道版本, 输入版本) 的在途运行合并，
  因此并发触发不会让同一步骤执行两遍，也不会互相覆盖状态。
- 每次状态变化经 LocalStore 追加一条快照，日志带 run_id / step / 判定依据。
- 运行级超时通过"剩余截止时间作为步骤超时上限 + 轮询取消信号"落地，
  取消/超时一定收敛到终止态，不会挂在 running。
"""
from __future__ import annotations

import logging
import threading
import time

from .datasource import LocalDataSource, utc_now
from .executor import StepExecutor
from .lineage import record_lineage
from .models import (
    FailureReason,
    PipelineDef,
    PipelineRun,
    RunStatus,
    StepRun,
    StepStatus,
    make_run_id,
    make_step_run_id,
)
from .store import LocalStore

logger = logging.getLogger("pipeline.engine")

POLL_INTERVAL = 0.05


class Orchestrator:
    def __init__(
        self,
        store: LocalStore | None = None,
        data_source: LocalDataSource | None = None,
        executor: StepExecutor | None = None,
    ) -> None:
        self.store = store or LocalStore()
        self.data_source = data_source or LocalDataSource()
        self.executor = executor or StepExecutor()
        self._pipeline_locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()
        self._cancel_events: dict[str, threading.Event] = {}
        self._completion_events: dict[str, threading.Event] = {}
        # "user_cancelled" / "run_timeout"
        self._abort_reason: dict[str, str] = {}
        # (pipeline_name, pipeline_version, 输入指纹) -> run_id
        self._inflight: dict[tuple[str, str, str], str] = {}

    # ------------------------------------------------------------------ 基础
    def _pipeline_lock(self, name: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._pipeline_locks.get(name)
            if lock is None:
                lock = threading.RLock()
                self._pipeline_locks[name] = lock
            return lock

    @staticmethod
    def _input_fingerprint(resolved: dict[str, str]) -> str:
        # resolved: {逻辑输入名: "ref@version"}
        return "|".join(f"{k}={v}" for k, v in sorted(resolved.items()))

    @staticmethod
    def _parse_ref(spec: str) -> tuple[str, str | None]:
        if "@" in spec:
            ref, _, version = spec.partition("@")
            return ref, version
        return spec, None

    def register_pipeline(self, pipeline: PipelineDef) -> None:
        """注册管道并校验 DAG（上游存在、无环）。"""
        names = {s.name for s in pipeline.steps}
        if len(names) != len(pipeline.steps):
            raise ValueError("duplicate step names")
        for step in pipeline.steps:
            missing = [u for u in step.upstream if u not in names]
            if missing:
                raise ValueError(
                    f"step {step.name!r} references unknown upstream {missing}"
                )
        # Kahn 拓扑排序同时承担环检测
        order = self._topo_order(pipeline)
        if len(order) != len(pipeline.steps):
            raise ValueError("pipeline graph contains a cycle")
        self.store.register_pipeline(pipeline)

    def _topo_order(self, pipeline: PipelineDef) -> list[str]:
        indegree = {s.name: len(s.upstream) for s in pipeline.steps}
        dependents: dict[str, list[str]] = {s.name: [] for s in pipeline.steps}
        for s in pipeline.steps:
            for u in s.upstream:
                dependents[u].append(s.name)
        ready = sorted(n for n, d in indegree.items() if d == 0)
        order: list[str] = []
        while ready:
            name = ready.pop(0)
            order.append(name)
            for dep in dependents[name]:
                indegree[dep] -= 1
                if indegree[dep] == 0:
                    ready.append(dep)
            ready.sort()
        return order

    # ------------------------------------------------------------------ 触发
    def trigger(
        self,
        pipeline_name: str,
        inputs: dict[str, str] | None = None,
        trigger: str = "manual",
        run_timeout_seconds: float | None = None,
        parent_run_id: str = "",
    ) -> str:
        pipeline = self.store.get_pipeline(pipeline_name)
        if pipeline is None:
            raise KeyError(f"unknown pipeline {pipeline_name!r}")

        # 立即把输入解析成具体版本并固定（ref 或 ref@version）
        resolved = self._resolve_inputs(inputs or {})
        fingerprint = self._input_fingerprint(resolved)
        key = (pipeline.name, pipeline.version, fingerprint)
        lock = self._pipeline_lock(pipeline.name)

        with lock:
            existing = self._inflight.get(key)
            if existing is not None:
                self._log_transition(existing, "-", "deduplicated",
                                     f"相同输入版本 {fingerprint} 的运行已在途，合并触发")
                return existing

            run = PipelineRun(
                id=make_run_id(),
                pipeline_name=pipeline.name,
                pipeline_version=pipeline.version,
                trigger=trigger,
                parent_run_id=parent_run_id,
                created_at=utc_now(),
                timeout_seconds=run_timeout_seconds,
                input_versions=dict(resolved),
            )
            for step_def in pipeline.steps:
                run.steps[step_def.name] = StepRun(
                    id=make_step_run_id(),
                    run_id=run.id,
                    step_name=step_def.name,
                )
            self.store.create_run(run)
            self._cancel_events[run.id] = threading.Event()
            self._completion_events[run.id] = threading.Event()
            self._inflight[key] = run.id
            self._log_transition(run.id, "-", "created",
                                 f"trigger={trigger} inputs={fingerprint or '<empty>'}")

        worker = threading.Thread(
            target=self._run_safely, args=(run.id, resolved, key),
            name=f"run-{run.id}", daemon=True,
        )
        worker.start()
        return run.id

    def _resolve_inputs(self, inputs: dict[str, str]) -> dict[str, str]:
        resolved: dict[str, str] = {}
        for logical_name, spec in inputs.items():
            ref, pinned = self._parse_ref(spec)
            version = pinned or self.data_source.current_version(ref)
            resolved[logical_name] = f"{ref}@{version}"
        return resolved

    # ------------------------------------------------------------------ 重试
    def retry(self, run_id: str) -> str:
        """基于一次终止态运行发起重试运行：复用已成功步骤的产出，不重复执行。"""
        parent = self.store.get_run(run_id)
        if parent is None:
            raise KeyError(f"unknown run {run_id!r}")
        if not parent.is_terminal():
            raise ValueError(f"run {run_id} is not terminal, cannot retry")
        if parent.status is RunStatus.SUCCEEDED:
            raise ValueError(f"run {run_id} already succeeded, nothing to retry")

        # 还原父运行的输入规格（ref@version 形式本身即固定版本）
        inputs = dict(parent.input_versions)
        new_run_id = self.trigger(
            parent.pipeline_name,
            inputs=inputs,
            trigger=f"retry-of:{run_id}",
            run_timeout_seconds=parent.timeout_seconds,
            parent_run_id=run_id,
        )

        # 把父运行中已成功的步骤标记为新运行的既有成果，执行循环将跳过它们
        with self._pipeline_lock(parent.pipeline_name):
            new_run = self.store.get_run(new_run_id)
            assert new_run is not None
            for name, parent_step in parent.steps.items():
                if parent_step.status is StepStatus.SUCCEEDED:
                    reused = StepRun(
                        id=make_step_run_id(),
                        run_id=new_run_id,
                        step_name=name,
                        status=StepStatus.SUCCEEDED,
                        attempt=parent_step.attempt,
                        started_at=parent_step.started_at,
                        finished_at=parent_step.finished_at,
                        input_versions=dict(parent_step.input_versions),
                        output_ref=parent_step.output_ref,
                        output_version=parent_step.output_version,
                    )
                    self.store.save_step(new_run_id, reused)
            self._log_transition(new_run_id, "-", "retry-seeded",
                                 f"复用父运行 {run_id} 的成功步骤产出，不重复执行")
        return new_run_id

    # ------------------------------------------------------------------ 取消
    def cancel(self, run_id: str) -> bool:
        """请求取消；对已结束的运行幂等。未知 run_id 返回 False。"""
        event = self._cancel_events.get(run_id)
        if event is not None:
            if not event.is_set():
                self._abort_reason.setdefault(run_id, "user_cancelled")
                event.set()
                self._log_transition(run_id, "-", "cancel-requested", "收到取消请求")
            return True
        # 事件已移除说明运行结束：仅当它确实以 CANCELLED 结束才视为幂等成功
        run = self.store.get_run(run_id)
        if run is None:
            return False
        return run.status is RunStatus.CANCELLED

    def wait(self, run_id: str, timeout: float | None = None) -> PipelineRun:
        event = self._completion_events.get(run_id)
        if event is not None:
            event.wait(timeout=timeout)
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"unknown run {run_id!r}")
        return run

    # ------------------------------------------------------------------ 执行
    def _run_safely(self, run_id: str, resolved_inputs: dict[str, str],
                    inflight_key: tuple[str, str, str]) -> None:
        try:
            self._execute(run_id, resolved_inputs)
        except Exception as exc:  # noqa: BLE001 - 兜底：任何异常都要让运行落到终止态
            logger.exception("[%s] 判定依据: 引擎未预期异常", run_id)
            if not self.store.get_run(run_id).is_terminal():
                self._terminate(run_id, RunStatus.FAILED, FailureReason.STEP_EXCEPTION,
                                f"engine error: {exc!r}")
        finally:
            with self._pipeline_lock(self.store.get_run(run_id).pipeline_name):
                self._inflight.pop(inflight_key, None)
            self._completion_events[run_id].set()
            self._cancel_events.pop(run_id, None)
            self._abort_reason.pop(run_id, None)

    def _execute(self, run_id: str, resolved_inputs: dict[str, str]) -> None:
        run = self.store.get_run(run_id)
        pipeline = self.store.get_pipeline(run.pipeline_name)
        assert pipeline is not None
        cancel_event = self._cancel_events[run_id]

        self.store.update_run_status(run_id, status=RunStatus.RUNNING, started_at=utc_now())
        self._log_transition(run_id, "-", RunStatus.RUNNING.value, "开始执行")

        # 读取固定版本的输入负载（历史版本不可变，覆盖/回滚不影响本次运行）
        input_payloads: dict[str, object] = {}
        for logical_name, spec in resolved_inputs.items():
            ref, version = self._parse_ref(spec)
            input_payloads[logical_name] = self.data_source.get(ref, version).payload

        deadline: float | None = None
        if run.timeout_seconds is not None:
            deadline = time.monotonic() + run.timeout_seconds

        failed = False
        for name in self._topo_order(pipeline):
            step_def = pipeline.step(name)
            assert step_def is not None
            current = self.store.get_run(run_id).steps[name]

            # 重试运行中已由父运行播种的成功步骤：直接复用，不再执行
            if current.status is StepStatus.SUCCEEDED:
                self._log_transition(run_id, name, StepStatus.SUCCEEDED.value,
                                     f"复用既有产出 {current.output_ref}@{current.output_version[:8]}，不重复执行")
                continue

            # 1) 取消 / 运行级超时优先判定
            abort = self._check_abort(run_id, deadline, name, step_def)
            if abort is not None:
                self._mark_remaining(run_id, pipeline, name, abort)
                failed = True
                break

            # 2) 上游未全部成功 -> 跳过（绝不当成成功）
            upstream_steps = self.store.get_run(run_id).steps
            bad_upstream = [u for u in step_def.upstream
                            if upstream_steps[u].status is not StepStatus.SUCCEEDED]
            if bad_upstream:
                self._save_step(
                    run_id, name, StepStatus.SKIPPED, FailureReason.UPSTREAM_FAILED,
                    f"upstream not succeeded: {', '.join(bad_upstream)}",
                    attempt=0,
                )
                self._log_transition(run_id, name, StepStatus.SKIPPED.value,
                                     f"上游 {bad_upstream} 未成功，明确跳过")
                continue

            # 3) 组装上下文并执行
            context = self._build_context(
                run_id, name, step_def, resolved_inputs, input_payloads, upstream_steps,
            )
            effective_timeout = self._effective_timeout(step_def.timeout_seconds, deadline)
            step_record = self.store.get_run(run_id).steps[name]
            started = utc_now()
            # 持久化 RUNNING 状态，使"步骤开始"也进入状态流转历史
            self._save_step(
                run_id, name, StepStatus.RUNNING, FailureReason.NONE, "",
                attempt=step_record.attempt + 1, started_at=started,
                input_versions=dict(context["_input_versions"]),
            )
            self._log_transition(run_id, name, StepStatus.RUNNING.value,
                                 f"attempt={step_record.attempt + 1}")
            result = self.executor.execute(
                step_def.handler, context, effective_timeout, cancel_event,
                run_id=run_id, step_name=name,
            )

            # 4) 执行返回后再次判定取消/超时（等待期间可能触发）
            abort = self._check_abort(run_id, deadline, name, step_def,
                                      ran_timeout=not result.ok and
                                      result.failure_reason is FailureReason.STEP_TIMEOUT)
            if abort is not None:
                self._mark_remaining(run_id, pipeline, name, abort)
                failed = True
                break

            if result.ok:
                output_ref = f"outputs/{pipeline.name}/{name}"
                record = self.data_source.put(
                    output_ref, result.payload,
                    producer=f"{pipeline.name}:{name}",
                )
                self._save_step(
                    run_id, name, StepStatus.SUCCEEDED, FailureReason.NONE, "",
                    attempt=step_record.attempt + 1, started_at=started,
                    finished_at=utc_now(), input_versions=dict(context["_input_versions"]),
                    output_ref=output_ref, output_version=record.version,
                )
                self._log_transition(run_id, name, StepStatus.SUCCEEDED.value,
                                     f"产出 {output_ref}@{record.version[:8]}")
            else:
                status = (StepStatus.TIMED_OUT
                          if result.failure_reason is FailureReason.STEP_TIMEOUT
                          else StepStatus.FAILED)
                self._save_step(
                    run_id, name, status, result.failure_reason, result.error_message,
                    attempt=step_record.attempt + 1, started_at=started,
                    finished_at=utc_now(), input_versions=dict(context["_input_versions"]),
                )
                self._log_transition(run_id, name, status.value,
                                     f"原因={result.failure_reason.value}: {result.error_message}")
                # 本步失败：下游全部明确跳过，运行失败
                self._mark_downstream_skipped(run_id, pipeline, name)
                self._terminate(run_id, RunStatus.FAILED, result.failure_reason,
                                f"step {name!r} {result.failure_reason.value}: "
                                f"{result.error_message}")
                failed = True
                break

        if not failed:
            self._terminate(run_id, RunStatus.SUCCEEDED, FailureReason.NONE, "")

    def _check_abort(self, run_id: str, deadline: float | None, step_name: str,
                     step_def, ran_timeout: bool = False):
        """返回 (run_status, step_status_for_current, reason, message) 或 None。"""
        reason = self._abort_reason.get(run_id)
        cancel_event = self._cancel_events[run_id]
        # 运行级超时优先级最高： watchdog/截止时间到了就算同时有取消请求，也记 TIMED_OUT
        if reason != "run_timeout" and cancel_event.is_set():
            return (RunStatus.CANCELLED, StepStatus.CANCELLED,
                    FailureReason.CANCELLED, "运行被取消")
        if deadline is not None and time.monotonic() >= deadline:
            self._abort_reason[run_id] = "run_timeout"
            cancel_event.set()
            return (RunStatus.TIMED_OUT, StepStatus.TIMED_OUT,
                    FailureReason.RUN_TIMEOUT,
                    f"运行超过截止时间，在步骤 {step_name} 处终止")
        if ran_timeout and deadline is not None and time.monotonic() >= deadline:
            return (RunStatus.TIMED_OUT, StepStatus.TIMED_OUT,
                    FailureReason.RUN_TIMEOUT, "运行级超时先于步骤判定")
        return None

    def _effective_timeout(self, step_timeout: float | None,
                           deadline: float | None) -> float | None:
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if step_timeout is None:
            return remaining
        if remaining is None:
            return step_timeout
        return min(step_timeout, remaining)

    def _build_context(self, run_id, step_name, step_def, resolved_inputs,
                       input_payloads, upstream_steps) -> dict:
        upstream_payloads: dict[str, object] = {}
        consumed_versions: dict[str, str] = {}
        for upstream_name in step_def.upstream:
            up = upstream_steps[upstream_name]
            dv = self.data_source.get(up.output_ref, up.output_version)
            upstream_payloads[upstream_name] = dv.payload
            consumed_versions[upstream_name] = up.output_version
        if not step_def.upstream:
            # 源步骤消费本次运行请求的固定版本输入
            for logical_name, spec in resolved_inputs.items():
                _, version = self._parse_ref(spec)
                consumed_versions[logical_name] = version

        return {
            "run_id": run_id,
            "step_name": step_name,
            "inputs": input_payloads,
            "upstream": upstream_payloads,
            "cancel_event": self._cancel_events[run_id],
            "_input_versions": consumed_versions,
        }

    def _mark_remaining(self, run_id: str, pipeline: PipelineDef, from_step: str,
                        abort) -> None:
        run_status, step_status, reason, message = abort
        order = self._topo_order(pipeline)
        idx = order.index(from_step)
        existing = self.store.get_run(run_id).steps[from_step]
        # 当前步骤可能尚未开始（attempt=0）或正在执行中（attempt>=1），保留其痕迹
        self._save_step(run_id, from_step, step_status, reason, message,
                        attempt=max(1, existing.attempt),
                        finished_at=utc_now())
        self._log_transition(run_id, from_step, step_status.value, f"判定依据: {message}")
        for name in order[idx + 1:]:
            self._save_step(run_id, name, StepStatus.SKIPPED, reason,
                            f"run {run_status.value} before step start", attempt=0,
                            finished_at=utc_now())
        self._terminate(run_id, run_status, reason, message)

    def _mark_downstream_skipped(self, run_id: str, pipeline: PipelineDef,
                                 failed_step: str) -> None:
        # 所有（传递）依赖失败步骤的步骤都跳过
        failed_deps = {failed_step}
        changed = True
        while changed:
            changed = False
            for step in pipeline.steps:
                if step.name in failed_deps:
                    continue
                if any(u in failed_deps for u in step.upstream):
                    failed_deps.add(step.name)
                    changed = True
        for name in self._topo_order(pipeline):
            if name in failed_deps and name != failed_step:
                self._save_step(
                    run_id, name, StepStatus.SKIPPED, FailureReason.UPSTREAM_FAILED,
                    f"upstream {failed_step!r} failed", attempt=0, finished_at=utc_now(),
                )
                self._log_transition(run_id, name, StepStatus.SKIPPED.value,
                                     f"判定依据: 上游 {failed_step} 失败，跳过")

    def _save_step(self, run_id: str, name: str, status: StepStatus,
                   reason: FailureReason, message: str, attempt: int,
                   started_at: str = "", finished_at: str = "",
                   input_versions: dict | None = None,
                   output_ref: str = "", output_version: str = "") -> None:
        existing = self.store.get_run(run_id).steps[name]
        step = StepRun(
            id=existing.id,
            run_id=run_id,
            step_name=name,
            status=status,
            attempt=attempt,
            failure_reason=reason,
            error_message=message,
            started_at=started_at or existing.started_at,
            finished_at=finished_at,
            input_versions=input_versions if input_versions is not None else dict(existing.input_versions),
            output_ref=output_ref or existing.output_ref,
            output_version=output_version or existing.output_version,
        )
        self.store.save_step(run_id, step)

    def _terminate(self, run_id: str, status: RunStatus, reason: FailureReason,
                   message: str) -> None:
        run = self.store.get_run(run_id)
        if run.is_terminal():
            return
        self.store.update_run_status(
            run_id, status=status, finished_at=utc_now(),
            failure_reason=reason, error_message=message,
        )
        self._log_transition(run_id, "-", status.value,
                             f"判定依据: {message or '全部步骤成功'}")
        record_lineage(self.store, self.store.get_run(run_id))

    def _log_transition(self, run_id: str, step_name: str, status: str,
                        reason: str) -> None:
        logger.info("[run=%s step=%s] -> %s | %s", run_id, step_name, status, reason)

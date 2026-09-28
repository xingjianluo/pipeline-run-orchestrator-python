"""本地状态存储：内存存储，全部本地、无外部服务。

并发与不可变保证：
- 所有读写都在同一把 RLock 下，运行状态以"整条快照追加"记录历史，
  历史快照永不就地修改，因此已完成运行的记录不会被后续回滚/覆盖改写。
"""
from __future__ import annotations

import copy
import threading
from dataclasses import asdict

from .models import LineageRecord, PipelineDef, PipelineRun, StepRun


class LocalStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._pipelines: dict[str, PipelineDef] = {}
        self._runs: dict[str, PipelineRun] = {}
        # run_id -> 每次状态变化的不可变快照列表
        self._run_history: dict[str, list[dict]] = {}
        self._lineage: list[LineageRecord] = []

    def register_pipeline(self, pipeline: PipelineDef) -> None:
        with self._lock:
            self._pipelines[pipeline.name] = pipeline

    def get_pipeline(self, name: str) -> PipelineDef | None:
        with self._lock:
            return self._pipelines.get(name)

    def create_run(self, run: PipelineRun) -> None:
        with self._lock:
            if run.id in self._runs:
                raise ValueError(f"run already exists: {run.id}")
            self._runs[run.id] = run
            self._run_history[run.id] = [self._snapshot(run)]

    def get_run(self, run_id: str) -> PipelineRun | None:
        with self._lock:
            run = self._runs.get(run_id)
            # 返回深拷贝，调用方拿到的对象与内部存储隔离，无法跨线程改写
            return copy.deepcopy(run) if run else None

    def list_runs(self, pipeline_name: str | None = None) -> list[PipelineRun]:
        with self._lock:
            runs = list(self._runs.values())
            if pipeline_name is not None:
                runs = [r for r in runs if r.pipeline_name == pipeline_name]
            return copy.deepcopy(runs)

    def save_step(self, run_id: str, step: StepRun) -> None:
        """把步骤状态深拷贝进运行，并追加一条不可变运行快照。"""
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(f"unknown run: {run_id}")
            run.steps[step.step_name] = copy.deepcopy(step)
            self._run_history[run_id].append(self._snapshot(run))

    def update_run_status(self, run_id: str, **changes: object) -> None:
        """更新运行标量字段并追加快照；不允许把终止态改回非终止态。"""
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(f"unknown run: {run_id}")
            if run.is_terminal() and changes.get("status") not in (None, run.status):
                raise ValueError(
                    f"run {run_id} already terminal ({run.status.value}), "
                    f"cannot move to {changes['status']}"
                )
            for key, value in changes.items():
                if not hasattr(run, key):
                    raise AttributeError(f"PipelineRun has no field {key!r}")
                setattr(run, key, value)
            self._run_history[run_id].append(self._snapshot(run))

    def run_history(self, run_id: str) -> list[dict]:
        """返回状态流转历史（每次步骤/运行变化一条快照）。"""
        with self._lock:
            return copy.deepcopy(self._run_history.get(run_id, []))

    def append_lineage(self, record: LineageRecord) -> None:
        with self._lock:
            for existing in self._lineage:
                if existing.run_id == record.run_id:
                    raise ValueError(f"lineage for run {record.run_id} already recorded")
            self._lineage.append(copy.deepcopy(record))

    def list_lineage(self, run_id: str | None = None) -> list[LineageRecord]:
        with self._lock:
            records = self._lineage
            if run_id is not None:
                records = [r for r in records if r.run_id == run_id]
            return copy.deepcopy(list(records))

    @staticmethod
    def _snapshot(run: PipelineRun) -> dict:
        snap = asdict(run)
        snap["status"] = run.status.value
        snap["failure_reason"] = run.failure_reason.value
        for step in snap["steps"].values():
            step["status"] = step["status"].value
            step["failure_reason"] = step["failure_reason"].value
        return snap

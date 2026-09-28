"""本地状态存储：版本化数据源与不可变运行记录（纯内存 + 锁）。

设计要点：
- 数据源按内容寻址：相同内容永远得到相同版本号（sha256 摘要），
  因此“相同版本的输入重新运行”结论一致、可复现。
- put/rollback 只移动“当前版本指针”，历史内容永不删除、不可改写；
  即使输入被覆盖或回滚，旧版本内容仍可按版本号读取。
- 运行记录到达终态后在 RunStore 中冻结，拒绝再次写入；
  读取一律返回深拷贝，外部无法篡改库内记录。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from typing import Any

from .models import PipelineRun, is_terminal, now_utc


def content_version(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    digest = hashlib.sha256(blob.encode("utf-8")).hexdigest()[:12]
    return f"v-{digest}"


class VersionedDataSource:
    """模拟数据源：写入产生不可变新版本，覆盖/回滚只移动当前版本指针。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # name -> {version: payload}，历史内容只增不改不删
        self._blobs: dict[str, dict[str, Any]] = {}
        # name -> 当前版本
        self._current: dict[str, str] = {}
        # name -> 指针移动流水 [(version, at, cause)]
        self._pointer_log: dict[str, list[tuple[str, str, str]]] = {}

    def put(self, name: str, payload: Any, cause: str = "put") -> str:
        version = content_version(payload)
        with self._lock:
            blobs = self._blobs.setdefault(name, {})
            if version not in blobs:
                blobs[version] = copy.deepcopy(payload)
            if self._current.get(name) != version:
                self._current[name] = version
                self._pointer_log.setdefault(name, []).append(
                    (version, now_utc(), cause)
                )
            return version

    def get(self, name: str, version: str | None = None) -> tuple[Any, str]:
        with self._lock:
            if version is None:
                version = self._current.get(name)
                if version is None:
                    raise KeyError(f"数据集不存在: {name}")
            blobs = self._blobs.get(name, {})
            if version not in blobs:
                raise KeyError(f"数据集 {name} 不存在版本 {version}")
            return copy.deepcopy(blobs[version]), version

    def current_version(self, name: str) -> str | None:
        with self._lock:
            return self._current.get(name)

    def exists(self, name: str, version: str) -> bool:
        with self._lock:
            return version in self._blobs.get(name, {})

    def rollback(self, name: str, version: str) -> str:
        with self._lock:
            if version not in self._blobs.get(name, {}):
                raise KeyError(f"数据集 {name} 不存在版本 {version}，无法回滚")
            if self._current.get(name) != version:
                self._current[name] = version
                self._pointer_log.setdefault(name, []).append(
                    (version, now_utc(), "rollback")
                )
            return version

    def pointer_history(self, name: str) -> list[tuple[str, str, str]]:
        with self._lock:
            return list(self._pointer_log.get(name, []))

    def history(self, name: str) -> list[str]:
        """该数据集曾经成为“当前版本”的版本号，按时间顺序。"""
        with self._lock:
            return [v for v, _at, _cause in self._pointer_log.get(name, [])]


class FrozenRunError(RuntimeError):
    """运行已到达终态并冻结，历史记录不允许改写。"""


class RunStore:
    """运行记录库：终态记录冻结，读取返回深拷贝，历史不可改写。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs: dict[str, PipelineRun] = {}

    def save(self, run: PipelineRun) -> None:
        with self._lock:
            old = self._runs.get(run.run_id)
            if old is not None and is_terminal(old.status):
                raise FrozenRunError(
                    f"运行 {run.run_id} 已处于终态 {old.status.value}，记录已冻结"
                )
            self._runs[run.run_id] = copy.deepcopy(run)

    def get(self, run_id: str) -> PipelineRun | None:
        with self._lock:
            run = self._runs.get(run_id)
            return copy.deepcopy(run) if run is not None else None

    def list_runs(self, pipeline_name: str | None = None) -> list[PipelineRun]:
        with self._lock:
            runs = self._runs.values()
            if pipeline_name is not None:
                runs = [r for r in runs if r.pipeline_name == pipeline_name]
            return [copy.deepcopy(r) for r in runs]

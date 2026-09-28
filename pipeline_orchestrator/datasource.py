"""本地版本化数据源：模拟上游任务产出的数据。

- 每次写入按内容生成 sha256 版本号；相同内容 -> 相同版本（幂等）。
- 某引用的全部历史版本都保留；覆盖写只新增版本并移动 current 指针。
- 回滚只把 current 指针指回历史版本（并留下回滚痕迹），不删除历史。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DataVersion:
    ref: str
    version: str
    producer: str
    created_at: str
    payload: Any


class LocalDataSource:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, list[DataVersion]] = {}

    def put(self, ref: str, payload: Any, producer: str = "", created_at: str = "") -> DataVersion:
        version = self.digest(payload)
        with self._lock:
            history = self._data.setdefault(ref, [])
            for dv in history:
                if dv.version == version:
                    # 内容相同即同一版本：不产生新版本，current 指针天然仍指向它
                    return copy.deepcopy(dv)
            record = DataVersion(
                ref=ref,
                version=version,
                producer=producer,
                created_at=created_at or utc_now(),
                payload=copy.deepcopy(payload),
            )
            history.append(record)
            return copy.deepcopy(record)

    def get(self, ref: str, version: str | None = None) -> DataVersion:
        with self._lock:
            history = self._data.get(ref)
            if not history:
                raise KeyError(f"no data for ref {ref!r}")
            target_version = version if version is not None else history[-1].version
            for dv in history:
                if dv.version == target_version:
                    return copy.deepcopy(dv)
            raise KeyError(f"ref {ref!r} has no version {version!r}")

    def current_version(self, ref: str) -> str:
        with self._lock:
            history = self._data.get(ref)
            if not history:
                raise KeyError(f"no data for ref {ref!r}")
            return history[-1].version

    def rollback(self, ref: str, version: str) -> DataVersion:
        """把 current 指针回滚到历史版本。

        通过追加一条同版本记录实现（旧 payload 重新成为 current），
        原始版本记录依旧在历史中、未被删除或改写。
        """
        with self._lock:
            history = self._data.get(ref)
            if not history:
                raise KeyError(f"no data for ref {ref!r}")
            target = next((dv for dv in history if dv.version == version), None)
            if target is None:
                raise KeyError(f"ref {ref!r} has no version {version!r}")
            record = DataVersion(
                ref=ref,
                version=target.version,
                producer=f"rollback:{target.producer}",
                created_at=utc_now(),
                payload=copy.deepcopy(target.payload),
            )
            history.append(record)
            return copy.deepcopy(record)

    def history(self, ref: str) -> list[DataVersion]:
        with self._lock:
            return copy.deepcopy(self._data.get(ref, []))

    @staticmethod
    def digest(payload: Any) -> str:
        blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]

"""步骤执行器：在线程池中执行步骤，支持步骤级超时与取消。

注意：Python 无法强杀线程，步骤超时后我们放弃等待（future 留给线程池自然结束），
引擎据此把该步骤判为 TIMED_OUT 并停止推进下游。
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from dataclasses import dataclass
from typing import Any, Callable

from .models import FailureReason

logger = logging.getLogger("pipeline.steps")

# 步骤处理函数签名: (context) -> 产出 payload
StepHandler = Callable[[dict[str, Any]], Any]


@dataclass
class StepResult:
    ok: bool
    payload: Any = None
    failure_reason: FailureReason = FailureReason.NONE
    error_message: str = ""


class StepExecutor:
    """用有界线程池执行步骤；超时/异常被翻译成可区分的 FailureReason。"""

    def __init__(self, max_workers: int = 8) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="step")

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    def execute(
        self,
        handler: StepHandler,
        context: dict[str, Any],
        timeout_seconds: float | None,
        cancel_event: Any,
        run_id: str = "",
        step_name: str = "",
    ) -> StepResult:
        """执行单个步骤。

        - cancel_event 被设置时直接返回 CANCELLED，不调用 handler。
        - 超过 timeout_seconds 返回 STEP_TIMEOUT。
        - handler 抛异常返回 STEP_EXCEPTION，错误信息保留用于区分原因。
        """
        if cancel_event is not None and cancel_event.is_set():
            logger.info("[%s] step=%s 判定依据: 运行已取消，跳过执行", run_id, step_name)
            return StepResult(False, failure_reason=FailureReason.CANCELLED,
                              error_message="run cancelled before step start")

        future = self._pool.submit(handler, context)
        try:
            payload = future.result(timeout=timeout_seconds)
        except FutureTimeout:
            logger.warning("[%s] step=%s 判定依据: 步骤超时 %.3fs",
                           run_id, step_name, timeout_seconds)
            future.cancel()
            return StepResult(
                False,
                failure_reason=FailureReason.STEP_TIMEOUT,
                error_message=f"step {step_name!r} timed out after {timeout_seconds}s",
            )
        except Exception as exc:  # noqa: BLE001 - 步骤异常需要转成可区分的失败结果
            logger.warning("[%s] step=%s 判定依据: 步骤异常 %r", run_id, step_name, exc)
            return StepResult(
                False,
                failure_reason=FailureReason.STEP_EXCEPTION,
                error_message=f"{type(exc).__name__}: {exc}",
            )
        return StepResult(True, payload=payload)

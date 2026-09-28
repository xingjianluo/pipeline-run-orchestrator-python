"""应用装配：创建单例 Orchestrator 并注册演示管道，供 main 与测试复用。"""
from __future__ import annotations

import logging
import os

from .datasource import LocalDataSource
from .engine import Orchestrator
from .executor import StepExecutor
from .pipelines import register_demo
from .store import LocalStore


class AppServices:
    def __init__(self) -> None:
        self.store = LocalStore()
        self.data_source = LocalDataSource()
        self.executor = StepExecutor(max_workers=8)
        self.orchestrator = Orchestrator(
            store=self.store, data_source=self.data_source, executor=self.executor,
        )


_services: AppServices | None = None


def get_services() -> AppServices:
    global _services
    if _services is None:
        _services = bootstrap_demo()
    return _services


def bootstrap_demo() -> AppServices:
    """写入一条模拟上游输入并注册演示管道，本地启动即可触发验证。

    可通过环境变量注入故障，方便不改代码验证：
      DEMO_FAIL_STEP / DEMO_FAIL_TIMES / DEMO_SLOW_STEP / DEMO_SLOW_SECONDS
    """
    logging.basicConfig(
        level=os.environ.get("PIPELINE_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    services = AppServices()
    services.data_source.put("demo/raw", [1, 2, 3, 4], producer="local-seed")

    fail_step = os.environ.get("DEMO_FAIL_STEP") or None
    fail_times = int(os.environ.get("DEMO_FAIL_TIMES", "1"))
    slow_step = os.environ.get("DEMO_SLOW_STEP") or None
    slow_seconds = float(os.environ.get("DEMO_SLOW_SECONDS", "1.0"))

    register_demo(
        services.orchestrator,
        services.data_source,
        fail_step=fail_step,
        fail_times=fail_times,
        slow_step=slow_step,
        slow_seconds=slow_seconds,
    )
    return services

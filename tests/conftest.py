"""测试夹具与判定日志：每个用例使用独立的 store/datasource/orchestrator。"""
import logging

import pytest

from pipeline_orchestrator.datasource import LocalDataSource
from pipeline_orchestrator.engine import Orchestrator
from pipeline_orchestrator.executor import StepExecutor
from pipeline_orchestrator.pipelines import register_demo
from pipeline_orchestrator.store import LocalStore

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")


@pytest.fixture
def env():
    """构造一套隔离环境；返回 (store, datasource, orchestrator, helpers)。"""
    store = LocalStore()
    ds = LocalDataSource()
    executor = StepExecutor(max_workers=4)
    orch = Orchestrator(store=store, data_source=ds, executor=executor)

    def setup(raw=(1, 2, 3, 4), **pipeline_kwargs):
        ds.put("demo/raw", list(raw), producer="test-seed")
        register_demo(orch, ds, **pipeline_kwargs)
        return store, ds, orch

    yield setup
    executor.shutdown()

"""HTTP API 端到端：用 FastAPI TestClient，无需真实监听端口或外部服务。"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

import main
from pipeline_orchestrator import service

from .helpers import poll_run


@pytest.fixture()
def client():
    # 每个测试使用全新的编排器实例，避免全局单例与故障开关串扰
    service.orchestrator = service.build_orchestrator()
    service.reset_scenarios()
    service.seed_raw_input(service.orchestrator)
    with TestClient(main.app) as c:
        yield c
    service.reset_scenarios()


def _trigger(client, **kwargs):
    resp = client.post("/api/pipelines/sample_pipeline/trigger", json=kwargs)
    assert resp.status_code == 200, resp.text
    return resp.json()["run_id"]


def test_full_lifecycle_over_http(client):
    rid = _trigger(client, trigger="http-success")
    detail = poll_run(client, rid)
    assert detail["status"] == "succeeded"
    lineage = client.get(f"/api/pipelines/runs/{rid}/lineage").json()
    assert set(lineage["outputs"]) == {"extract", "transform", "load"}
    # 根路由仍可用，说明新能力与原有服务共存
    assert client.get("/").json() == {"Hello": "World"}


def test_failure_retry_over_http(client):
    client.post("/api/pipelines/scenarios", json={"fail_step": "transform"})
    rid = _trigger(client)
    detail = poll_run(client, rid)
    assert detail["status"] == "failed"
    assert detail["steps"]["load"]["status"] == "skipped"
    assert detail["steps"]["load"]["reason"] == "upstream_failed"

    client.post("/api/pipelines/scenarios/reset")
    resp = client.post(f"/api/pipelines/runs/{rid}/retry", json={})
    assert resp.status_code == 200
    rid2 = resp.json()["run_id"]
    detail2 = poll_run(client, rid2)
    assert detail2["status"] == "succeeded"
    assert detail2["steps"]["extract"]["attempts"] == 0  # 复用，不重跑


def test_timeout_and_cancel_over_http(client):
    client.post("/api/pipelines/scenarios", json={"slow_step": "extract", "slow_seconds": 3})
    rid = _trigger(client, timeout=0.6)
    time.sleep(1.0)
    detail = poll_run(client, rid)
    assert detail["status"] == "timed_out"
    assert detail["steps"]["extract"]["reason"] == "timed_out"

    client.post("/api/pipelines/scenarios", json={"slow_step": "transform", "slow_seconds": 3})
    rid2 = _trigger(client)
    time.sleep(0.4)
    assert client.post(f"/api/pipelines/runs/{rid2}/cancel").status_code == 200
    time.sleep(0.5)
    detail2 = poll_run(client, rid2)
    assert detail2["status"] == "cancelled"
    assert detail2["steps"]["transform"]["reason"] == "cancelled"
    client.post("/api/pipelines/scenarios/reset")


def test_idem_key_and_unknown_pipeline(client):
    ids = {
        _trigger(client, idem_key="http-idem")
        for _ in range(4)
    }
    assert len(ids) == 1
    resp = client.post("/api/pipelines/nope/trigger", json={})
    assert resp.status_code == 404

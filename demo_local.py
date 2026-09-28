"""本地一键演示：成功、失败跳过、重试、超时、取消、血缘与回滚。

运行方式：uv run python demo_local.py
全部使用进程内状态，不依赖任何外部服务。
"""
from __future__ import annotations

import logging
import time

from pipeline_orchestrator import service
from pipeline_orchestrator.models import RunStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def show(title, run):
    steps = {n: (s.status.value, s.reason.value) for n, s in run.steps.items()}
    print(f"\n=== {title} ===\nrun={run.run_id} status={run.status.value} steps={steps}")


def main() -> None:
    service.reset_scenarios()
    orch = service.build_orchestrator(default_timeout=5.0)
    service.seed_raw_input(orch)

    # 1) 正常运行
    ok = orch.wait(orch.trigger("sample_pipeline", trigger="demo-ok"), 5)
    show("1. 正常运行", ok)
    print("血缘:", orch.lineage(ok.run_id))

    # 2) transform 失败，load 被跳过
    service.set_fail_step("transform")
    bad = orch.wait(orch.trigger("sample_pipeline", trigger="demo-fail"), 5)
    show("2. 步骤失败 + 下游跳过", bad)

    # 3) 清故障重试，extract 复用
    service.set_fail_step(None)
    retried = orch.wait(orch.retry(bad.run_id, trigger="demo-retry"), 5)
    show("3. 重试（复用已成功步骤）", retried)

    # 4) 超时
    service.set_slow_step("extract", 3.0)
    timed = orch.wait(
        orch.trigger("sample_pipeline", trigger="demo-timeout", timeout=0.6), 5
    )
    show("4. 超时", timed)

    # 5) 取消
    service.set_slow_step("transform", 3.0)
    rid = orch.trigger("sample_pipeline", trigger="demo-cancel")
    time.sleep(0.3)
    orch.cancel(rid)
    cancelled = orch.wait(rid, 5)
    show("5. 取消", cancelled)

    service.reset_scenarios()
    assert all(
        r.status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                     RunStatus.TIMED_OUT, RunStatus.CANCELLED)
        for r in (ok, bad, retried, timed, cancelled)
    )
    print("\n全部演示完成：所有运行均处于终止态。")


if __name__ == "__main__":
    main()

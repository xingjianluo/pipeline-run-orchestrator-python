# 本地数据管道编排（Pipeline Run Orchestrator）

一套**完全本地**的数据管道运行编排能力，模拟"上游任务产出数据、下游任务接着跑"的场景。
不依赖任何外部服务、真实凭据或付费组件：数据源、调度、状态存储全部是进程内实现，
并随现有 FastAPI 服务（`main.py`）一起启动。

## 目录结构

```
pipeline_orchestrator/
  models.py      状态枚举与领域模型（PipelineDef / PipelineRun / StepRun / LineageRecord）
  store.py       线程安全的本地存储，状态流转"整条快照追加"，历史不可变
  datasource.py  本地版本化数据源（内容寻址版本、覆盖写、回滚）
  executor.py    步骤执行器（线程池、步骤级超时、取消感知）
  engine.py      编排引擎（DAG 推进、并发去重、重试、取消、运行级超时）
  lineage.py     血缘记录构建（输入版本 / 产出 / 触发方式 / 父运行）
  pipelines.py   内置演示管道 demo
  api.py         HTTP API（触发 / 查询 / 重试 / 取消 / 历史 / 血缘）
  service.py     装配与本地引导（写入种子数据、注册演示管道）
tests/           pytest 测试：并发、重试、上游失败、取消/超时、血缘
main.py          FastAPI 入口，挂载编排 API
```

## 演示管道与依赖规则

内置 `demo` 管道（版本 `1.0.0`）：

```
ingest (源步骤，消费输入 raw)
  ├──> transform ──┐
  └──> enrich ──────┴──> aggregate
```

- 管道注册时做 DAG 校验：上游必须存在、图不能有环。
- 步骤按拓扑序推进：**所有上游步骤都 `succeeded` 才允许执行**。
- 某个步骤失败时，所有（传递）依赖它的下游步骤被标记为 **`skipped`**（原因
  `upstream_failed`），不会被当成成功；与失败分支无关的独立分支（如 `enrich`）照常执行。
- 运行聚合状态：全部成功 → `succeeded`；任一步骤失败 → `failed`；取消 → `cancelled`；
  运行级超时 → `timed_out`。终止态之后不能再被改回非终止态。

### 运行 / 步骤状态

| 维度 | 状态 |
| --- | --- |
| Run | `pending` → `running` → `succeeded` / `failed` / `cancelled` / `timed_out` |
| Step | `pending` → `running` → `succeeded` / `failed` / `skipped` / `timed_out` / `cancelled` |

### 可区分的失败原因 `FailureReason`

- `step_exception`：步骤处理函数抛异常（错误消息保留异常类型与内容，可区分不同失败）
- `upstream_failed`：上游未成功，本步骤明确跳过
- `step_timeout`：步骤执行超过自身 `timeout_seconds`
- `run_timeout`：整次运行超过 `run_timeout_seconds`
- `cancelled`：运行被取消

## 并发与重复触发保证

- 每个管道一把 `RLock`，触发动作串行化。
- 相同 `(管道版本, 输入版本集合)` 的**在途**运行会被合并：并发/重复触发返回同一个
  `run_id`，任何步骤都不会执行第二遍，状态也不会互相覆盖。
- 不同输入版本的运行彼此独立，各自有独立 `run_id`、状态历史与血缘。
- 每次状态变化都会向运行历史追加一条完整快照，并打印日志，格式形如：
  `[run=<run_id> step=<步骤名>] -> <新状态> | <判定依据>`。

## 重试策略

- 只允许对**终止态且非成功**的运行重试：`POST /api/pipelines/runs/{id}/retry`。
- 重试会创建一条**新运行**（`trigger=retry-of:<父run_id>`，血缘记录 `parent_run_id`），
  父运行记录保持原样不被改写。
- 新运行**复用父运行中已成功步骤的产出版本，不重新执行**；只对失败/跳过的步骤重新执行。
- 输入按父运行固定的 `ref@version` 解析，重试结论可复现。

## 超时与取消策略

- 步骤级超时：`StepDef.timeout_seconds`，超时步骤记 `timed_out / step_timeout`，
  下游跳过，运行失败。
- 运行级超时：触发时传 `run_timeout_seconds`。引擎把"运行剩余时间"作为每个步骤的
  实际上限，并在步骤边界检查截止时间；超时后未开始的步骤记 `skipped / run_timeout`，
  运行落到 `timed_out`。
- 取消：`POST .../cancel` 设置取消信号；源步骤/慢步骤在分段 sleep 中响应取消，
  运行落到 `cancelled`，当前步骤记 `cancelled`，未开始步骤记 `skipped`。
- 兜底：引擎工作线程捕获任何未预期异常也会把运行收敛到终止态，不会挂在 `running`。

## 血缘与版本追溯

- 数据源是**内容寻址**的：写入内容的 sha256 前 16 位即版本号，相同内容得到相同版本。
  覆盖写只新增版本并移动 current 指针；回滚只把 current 指回历史版本，历史数据永不删除。
- 触发时输入立即解析为固定的 `ref@version`，运行全程读固定版本，因此：
  - 相同版本输入重新运行，得到一致结论与相同产出版本；
  - 输入事后被覆盖或回滚，已完成运行的状态、历史快照、血缘都不会被改写。
- 每次运行终止时追加一条不可变血缘记录：
  - 管道名/版本、触发方式（manual / retry-of / api-call…）、父运行；
  - 每个逻辑输入的 `ref@version`；
  - 每个步骤的产出引用、产出版本、状态与失败原因。
- 查询：`GET /api/pipelines/runs/{id}/history`（逐次状态流转）与
  `GET /api/pipelines/runs/{id}/lineage`（血缘）。

## 本地跑起来

要求 Python 3.14+（与 `pyproject.toml` 一致），用 uv：

```bash
uv sync
uv run python main.py            # http://127.0.0.1:8000 ，文档 /docs
```

触发一次演示运行并查看结果：

```bash
curl -s -X POST localhost:8000/api/pipelines/trigger \
  -H 'content-type: application/json' \
  -d '{"pipeline":"demo","inputs":{"raw":"demo/raw"}}'
# => {"run_id":"run_xxx"}

curl -s localhost:8000/api/pipelines/runs/<run_id>          # 状态与每步结果
curl -s localhost:8000/api/pipelines/runs/<run_id>/history  # 状态流转快照
curl -s localhost:8000/api/pipelines/runs/<run_id>/lineage  # 血缘与版本
```

验证失败/慢步骤场景（不改代码，用环境变量）：

```bash
# transform 前 1 次执行失败，用于看重试；前 N 次失败用 DEMO_FAIL_TIMES
DEMO_FAIL_STEP=transform DEMO_FAIL_TIMES=1 uv run python main.py
# ingest 人为变慢，用于在另一终端调 cancel 或触发时带 run_timeout_seconds
DEMO_SLOW_STEP=ingest DEMO_SLOW_SECONDS=10 uv run python main.py

curl -s -X POST localhost:8000/api/pipelines/trigger -H 'content-type: application/json' \
  -d '{"pipeline":"demo","inputs":{"raw":"demo/raw"},"run_timeout_seconds":1}'
curl -s -X POST localhost:8000/api/pipelines/runs/<run_id>/cancel
curl -s -X POST localhost:8000/api/pipelines/runs/<failed_run_id>/retry
```

## 运行测试

```bash
uv run pytest -s                      # -s 可看到带 run_id/步骤名/判定依据的日志
uv run pytest tests -q
```

覆盖的场景：

- `tests/test_concurrency.py`：并发相同输入合并、不同输入独立、步骤不重复执行；
- `tests/test_retry.py`：失败原因可区分、下游明确跳过、重试不重跑成功步骤；
- `tests/test_lineage.py`：输入/产出版本记录、相同输入一致结论、覆盖与回滚不改写历史；
- `tests/test_timeout_cancel.py`：步骤超时、运行级超时、取消均落到终止态。

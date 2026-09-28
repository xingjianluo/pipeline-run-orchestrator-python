# 本地数据管道编排（Pipeline Run Orchestrator）

在原有 FastAPI 服务上新增一套**纯本地**的数据管道运行编排能力，模拟
“上游任务产出数据、下游任务接着跑”的场景。数据源与调度全部用进程内状态模拟，
**不依赖任何外部服务、真实凭据或付费组件**，启动即可用、可验证。

## 一、包含什么

```
pipeline_orchestrator/
  models.py    领域模型：运行/步骤状态、失败原因、事件、血缘、序列化
  store.py     本地存储：内容寻址的版本化数据源 + 终态冻结的运行记录库
  executor.py  执行引擎：依赖调度、并发互斥、重试、超时、取消
  service.py   装配：内置示例管道 sample_pipeline（extract→transform→load）
  api.py       FastAPI 路由（挂在 /api/pipelines 前缀下，与原服务共存）
tests/         并发触发 / 重试 / 上游失败 / 取消超时 / 血缘版本 / HTTP 端到端
demo_local.py  一键演示全部场景
main.py        应用入口，include 管道路由
```

示例管道依赖关系：

```
extract ──▶ transform ──▶ load
(读 raw_input) (消费 extract 产物) (消费 transform 产物)
```

## 二、依赖与状态流转规则

### 步骤状态（StepStatus）

`pending → running → succeeded`，失败分支为 `running → failed`，
未执行的下游为 `pending → skipped`。

### 运行状态（RunStatus）

`pending → running → succeeded | failed | cancelled | timed_out`。

### 调度规则

- 启动时对管道做拓扑排序（Kahn），并校验“依赖必须存在、不能有环、每个步骤都有处理函数”。
- 一个步骤只有在**所有上游都 succeeded** 时才会被调度执行。
- 步骤状态 `pending → running` 是在“每运行一把锁”下的**原子领取**，
  同一步骤不可能被执行两遍；每次运行只在单条调度线程内推进。
- 任何上游 `failed/skipped`，下游一律置为 `skipped`（**不会被当成成功**），
  并区分原因：直接上游失败记 `upstream_failed`，上游本身也是跳过记 `upstream_skipped`。
- 每次状态变化都追加一条事件（`RunEvent`），带 `run_id`、`step`、
  `from/to_status`、`reason`、时间戳和单调序号，可清楚看出“属于哪个运行、哪一步、为什么”。

### 并发触发去重

- 指定相同 `run_id` 的触发（含并发）合并为同一个运行。
- 相同 `idem_key`（幂等键）的触发**永久**去重：无论首个运行进行中还是已结束，
  同键始终返回同一个 `run_id`，不会把任何步骤再跑一遍。
- 不带 `run_id/idem_key` 的触发彼此独立，各自有独立运行标识，状态互不覆盖。

## 三、失败处理、重试、超时与取消策略

### 失败原因（FailureReason，可区分）

| 原因 | 含义 |
| --- | --- |
| `step_error` | 步骤处理函数自身抛错，`detail` 记录异常类型与消息 |
| `upstream_failed` | 直接上游步骤失败，本步骤被跳过 |
| `upstream_skipped` | 上游也是被跳过的，本步骤级联跳过 |
| `cancelled` | 运行被取消（步骤执行中或等待中） |
| `timed_out` | 步骤执行超过该运行的截止时间 |

### 重试

- 只有处于终态且**非成功**的运行可以重试；重试产生**新的运行**，
  新运行 `trigger` 形如 `retry:<原run_id>`，可回溯来源。
- 重试时原运行已 `succeeded` 的步骤**直接复用其产物版本，不重新执行**
  （这些步骤 `attempts=0`，并留一条“复用”事件）；仅失败/被跳过的步骤重新执行。
- 原始失败运行记录保持 `failed` 且在存储层冻结，不被改写。

### 超时

- 每次触发可用 `timeout` 指定截止时间（默认 5s）。步骤在子线程执行，
  调度器轮询感知截止时间：超时后执行中的步骤判 `failed/timed_out`，
  其余未决步骤 `skipped/timed_out`，运行整体落 `timed_out`，不会挂在 running。
- 超时后即便工作线程稍后才真正结束，也无法再回写已终态的步骤/运行。

### 取消

- `cancel(run_id)` 设置协作式取消信号；处理函数可调用
  `ctx.raise_if_cancelled()` 及时响应（示例慢步骤按 50ms 分片睡眠）。
- 取消后执行中的步骤判 `failed/cancelled`，下游 `skipped/cancelled`，
  运行整体落 `cancelled`；对已结束运行重复取消返回 `false`。

## 四、血缘与版本追溯

### 内容寻址的版本化数据源

- 数据集版本号 = 内容的 sha256 摘要（`v-xxxxxxxxxxxx`）。
  **相同内容永远是同一版本**，因此相同版本输入重跑，产物版本与结论一致、可复现。
- `put` 只是在“当前版本指针”上前进；`rollback` 只把指针拨回旧版本。
  历史版本的内容**只增不改不删**，随时可按版本号读取。
- 每次运行在开始/读取时把所用输入的**具体版本号**固定到
  `run.input_versions`，运行中不会因外部覆盖而读到别的版本。

### 运行血缘（Lineage）

`GET /api/pipelines/runs/{run_id}/lineage` 返回：

- `trigger`：由什么触发（手动 / 重试来源 / 自定义标识）；
- `input_versions`：本次运行使用了哪个版本的每个输入；
- `outputs`：每个成功步骤产出了哪个版本的数据集。

### 历史不可改写

- 运行进入终态后在 `RunStore` 中**冻结**，再次 `save` 抛出 `FrozenRunError`。
- 因此输入被覆盖、版本被回滚后，**已经完成的运行记录与血缘保持原样**，
  可随时审计“当时用了什么版本、产出了什么”。

## 五、本地怎么跑起来验证

需要 Python 3.14 与 [uv](https://docs.astral.sh/uv/)（仓库已带 `uv.lock`）。

### 1. 安装依赖

```bash
uv sync
```

### 2. 跑测试（覆盖并发触发 / 重试 / 上游失败 / 取消超时 / 血缘）

```bash
uv run pytest
```

测试日志会打印每个断言对应的**运行标识、步骤状态、attempts 与判定依据**，例如：

```
判定 run_id=run-xxx run_status=failed
步骤[extract=succeeded/none/attempts=1, transform=failed/step_error/attempts=1,
     load=skipped/upstream_failed/attempts=0]
依据=transform 失败后停止；load 明确 skipped/upstream_failed 而非成功
```

### 3. 一键演示（无需测试框架）

```bash
uv run python demo_local.py
```

### 4. 启动 HTTP 服务（与原有接口共存）

```bash
uv run uvicorn main:app --host 127.0.0.1 --port 8000
# 或 uv run python main.py
```

常用接口：

```bash
# 管道定义
curl http://127.0.0.1:8000/api/pipelines

# 触发（可带 run_id / idem_key / timeout）
curl -X POST http://127.0.0.1:8000/api/pipelines/sample_pipeline/trigger \
  -H 'Content-Type: application/json' -d '{"trigger":"manual"}'

# 查询运行详情（含每步状态、原因、事件流水）
curl http://127.0.0.1:8000/api/pipelines/runs/<run_id>

# 查询血缘
curl http://127.0.0.1:8000/api/pipelines/runs/<run_id>/lineage

# 重试 / 取消
curl -X POST http://127.0.0.1:8000/api/pipelines/runs/<run_id>/retry -d '{}' -H 'Content-Type: application/json'
curl -X POST http://127.0.0.1:8000/api/pipelines/runs/<run_id>/cancel

# 本地故障注入：让某步骤失败 / 变慢（用于验证重试、超时、取消）
curl -X POST http://127.0.0.1:8000/api/pipelines/scenarios \
  -H 'Content-Type: application/json' -d '{"fail_step":"transform"}'
curl -X POST http://127.0.0.1:8000/api/pipelines/scenarios \
  -H 'Content-Type: application/json' -d '{"slow_step":"extract","slow_seconds":3}'
curl -X POST http://127.0.0.1:8000/api/pipelines/scenarios/reset

# 重新写入一份模拟原始数据（默认 5 条记录）
curl -X POST http://127.0.0.1:8000/api/pipelines/seed \
  -H 'Content-Type: application/json' -d '{"n_records":10}'
```

交互式文档：启动后访问 `http://127.0.0.1:8000/docs`。

> 说明：所有运行记录、版本数据都保存在进程内存中，重启服务即清空，
> 这是刻意为之的“本地模拟”，便于反复验证且不产生任何外部依赖。

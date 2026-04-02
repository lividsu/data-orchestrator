# Data Orchestrator

> 轻量级数据编排框架。把所有"定期去某个地方拿数据、处理、再放到某个地方"的事情，统一管理起来。

---

## 目录

- [为什么要用这个](#为什么要用这个)
- [核心特性](#核心特性)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [编写 Connector](#编写-connector)
- [编写 Pipeline (YAML)](#编写-pipeline-yaml)
- [调度配置](#调度配置)
- [数据流传递](#数据流传递)
- [重试与超时](#重试与超时)
- [告警通知](#告警通知)
- [Web UI 界面](#web-ui-界面)
- [命令行与 API 管理](#命令行与-api-管理)
- [内置 Connector](#内置-connector)
- [与现有方案对比](#与现有方案对比)

---

## 为什么要用这个

你的数据任务大概是这个样子：

```
❌ 现在
────────────────────────────────────────────────────────────
scripts/pull_orders.py       ← 每天手动跑，或者一个 crontab
scripts/sync_feishu.py       ← 偶尔失败，你不知道
scripts/generate_report.py   ← 上次跑成功是什么时候？忘了
────────────────────────────────────────────────────────────
没有重试  没有日志  没有告警  出了问题靠命
```

```
✅ 用 Orchestrator 之后
────────────────────────────────────────────────────────────
pipelines/
  daily_sync.yaml            ← 描述你要做什么
  weekly_report.yaml
connectors/
  shopify.py                 ← 一次编写，永久复用
  feishu.py

$ orchestrator run           ← 启动，自带 Web UI
────────────────────────────────────────────────────────────
✓ 定时自动触发               ✓ 失败自动重试、超时控制
✓ DAG 并发执行与数据流传递     ✓ Web UI 可视化面板与日志检索
✓ HTTP API 调度管控          ✓ 飞书告警通知防抖
```

---

## 核心特性

- **业务与框架分离**：框架只提供骨架（调度、重试、日志、UI、API），Connector 和 Pipeline 完全放在你的项目中独立维护。升级框架不影响你的业务代码。
- **YAML 编排驱动**：支持任务 DAG 依赖（`depends_on`），可控制全局并发与失败阻断策略。
- **数据流传递**：支持上游任务输出无缝作为下游任务的输入（`pass_output_from`）。
- **运行时动态模板**：使用 Jinja 模板（如 `{{ today }}`、`{{ yesterday_iso }}`、`{{ run_id }}`），并在执行时而非加载时动态渲染。
- **内置 Web UI 与 API**：基于 Streamlit 的可视化看板，并内置轻量级 HTTP API，支持外部系统进行触发管控。
- **Pipeline 级 Hook**：支持 `on_success` 和 `on_failure`，轻松实现流水线串联或回调通知。
- **连接器生命周期管理**：连接器在一次 Pipeline 执行中跨 Task 自动复用，提供同步及默认的 `async_fetch` / `async_push` 异步封装。

---

## 快速开始

### 1. 安装

```bash
pip install data-orchestrator[ui,postgres]
```

*(可选：仅安装核心依赖 `pip install data-orchestrator`)*

### 2. 初始化项目

使用 `init` 命令快速生成骨架代码：

```bash
orchestrator init my_data_project
cd my_data_project
```

这将自动生成如下目录结构：
```
my_data_project/
├── connectors/
│   └── demo.py       # 你的自定义逻辑
├── pipelines/
│   └── demo.yaml     # 任务编排配置
└── main.py           # 启动脚本
```

### 3. 启动运行

```bash
# 通过 CLI 启动
orchestrator run --config pipelines/ --plugins connectors/

# 或者直接运行 main.py
python main.py
```

启动后，访问 **http://localhost:8501** 即可看到内置的可视化看板。

---

## 核心概念

```
Connector   ── 知道怎么和某个系统交互。不仅限 fetch / push，
               你可以定义任意 action（如 send_report）。
    ↓
Task        ── 用某个 Connector 执行具体的动作（拉数据/推数据）
    ↓
Pipeline    ── 将多个 Task 按 DAG（有向无环图）组合成一条数据链路
    ↓
Scheduler   ── 决定 Pipeline 的执行策略（cron / interval / manual）
    ↓
UI / API    ── 可视化与管控接口
```

**核心设计原则：框架提供骨架，业务代码在框架外。**
框架只负责调度、重试、日志、通知这些通用能力。你的 Connector 实现、YAML 配置，完全放在自己的项目里，通过注册机制接入框架。

---

## 编写 Connector

所有 Connector 继承 `BaseConnector`，只需实现自己用到的方法。
框架通过 YAML 里的 `action` 字段按名称调用方法，因此 Connector 上的任何公开方法都可以作为 action。`fetch` / `push` / `ping` 是常见约定，但不是强制要求。

```python
# connectors/shopify.py
import requests
from orchestrator import BaseConnector, register_connector

@register_connector("shopify")
class ShopifyConnector(BaseConnector):

    def __init__(self, config: dict):
        super().__init__(config)
        self.shop_url = config["shop_url"]
        self.access_token = config["access_token"]
        self.session = requests.Session()
        self.session.headers.update({"X-Shopify-Access-Token": self.access_token})

    def fetch(self, endpoint: str, params: dict = None, **kwargs):
        """拉取数据：返回值将作为 TaskResult.output 传递给下游"""
        url = f"{self.shop_url}/admin/api/2024-01/{endpoint}"
        r = self.session.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def push(self, data, endpoint: str, **kwargs):
        """推送数据：data 参数可以通过 pass_output_from 由上游 Task 传入"""
        url = f"{self.shop_url}/admin/api/2024-01/{endpoint}"
        self.session.post(url, json=data)

    def ping(self) -> bool:
        """连通性健康检查：返回 True/False，不要抛异常。框架启动时进行验证"""
        try:
            r = self.session.get(f"{self.shop_url}/admin/api/2024-01/shop.json", timeout=5)
            return r.status_code == 200
        except Exception:
            return False
```

*(提示：Pipeline 执行过程中，框架会缓存并复用配置相同的 Connector 实例，执行完毕后统一调用 `close()`，降低连接建立开销)*

### 自定义 Action

不是所有场景都适合 `fetch` / `push` 的语义。你可以定义任意方法名：

```python
import requests
from orchestrator import BaseConnector, register_connector

@register_connector("feishu_daily")
class FeishuDailyConnector(BaseConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.webhook_url = config["webhook_url"]

    def send_report(self, title: str = "日报", content: str = "", **kwargs):
        payload = {"msg_type": "text", "content": {"text": f"{title}\n{content}"}}
        resp = requests.post(self.webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
```

在 YAML 里对应的使用方式：

```yaml
tasks:
  - id: send_daily
    connector: feishu_daily
    action: send_report
    action_kwargs:
      title: "{{ today }} 日报"
      content: "数据同步完成"
```

### Connector 配置安全

Connector 的敏感配置（token、密码）建议写在 `.env` 中，YAML 里使用 `${VAR_NAME}` 引用：

```yaml
connector_config:
  shop_url: "${SHOPIFY_SHOP_URL}"
  access_token: "${SHOPIFY_ACCESS_TOKEN}"
```

---

## 编写 Pipeline (YAML)

在 `pipelines/` 目录下创建 YAML 文件来编排你的任务：

```yaml
pipelines:
  - id: daily_order_sync                 # 唯一 ID，CLI 触发时用
    name: 每日订单同步                    # 可读名称，显示在 UI 里
    description: 从 Shopify 拉取昨日订单，写入 Postgres
    
    # 调度配置：cron / interval / manual
    schedule:
      type: cron
      cron_expr: "0 6 * * *"
      timezone: "Asia/Shanghai"

    # Pipeline 级别设置
    max_concurrency: 4         # 无依赖任务的最大并发数
    stop_on_failure: true      # 某个 Task 失败后，是否停止后续 Task
    
    # 钩子（可选），可串联流水线或触发回调
    on_success: "trigger:another_pipeline_id"
    on_failure: "my_module:my_alert_function"

    # 通知配置（见下方"告警通知"章节）
    notify:
      on_task_failure: true
      channels: 
        - name: feishu
          config:
            webhook_url: "${FEISHU_WEBHOOK}"

    tasks:
      - id: fetch_orders                 # Task ID，depends_on 里引用这个
        name: 拉取订单
        connector: shopify               # 对应 @register_connector("shopify")
        connector_config:
          shop_url: "${SHOPIFY_URL}"
          access_token: "${SHOPIFY_TOKEN}"
        action: fetch                    # Connector 上的任意方法名
        action_kwargs:                   # 传给 connector.fetch() 的参数
          endpoint: orders.json
          params:
            created_at_min: "{{ yesterday_iso }}" # 运行时动态渲染内置变量
        retry:
          times: 3
          delay_seconds: 10
          backoff: 2.0                   # 指数退避：10s → 20s → 40s
        timeout_seconds: 60

      - id: write_to_postgres
        name: 写入数据库
        connector: postgres
        connector_config:
          dsn: "${DATABASE_URL}"
        action: push
        depends_on: [fetch_orders]       # 依赖 fetch_orders 完成后才执行
        pass_output_from: fetch_orders   # 把 fetch_orders 的输出作为 data 传入
        action_kwargs:
          table: shopify_orders
          mode: upsert
          upsert_key: order_id
```

### 内置模板变量

在 `action_kwargs` 中可以使用 Jinja 语法注入时间上下文，**在任务实际执行时计算**（非启动时计算，避免长驻进程时间不更新的问题）：

| 变量 | 含义 | 示例值 |
|---|---|---|
| `{{ now }}` | 当前时间（ISO 8601） | `2024-03-15T06:00:00+08:00` |
| `{{ today }}` | 今天日期 | `2024-03-15` |
| `{{ yesterday }}` | 昨天日期 | `2024-03-14` |
| `{{ yesterday_iso }}` | 昨天 ISO 格式 | `2024-03-14T00:00:00+08:00` |
| `{{ run_id }}` | 本次执行的唯一 ID | `run_20240315_060001_a3f2` |
| `{{ pipeline_id }}` | Pipeline ID | `daily_order_sync` |
| `{{ week_start }}` | 本周一日期 | `2024-03-11` |
| `{{ month_start }}` | 本月 1 号 | `2024-03-01` |

---

## 调度配置

### Cron 触发

```yaml
schedule:
  type: cron
  cron_expr: "0 6 * * *"       # 每天早上 6:00
  timezone: "Asia/Shanghai"    # 默认 Asia/Shanghai
  start_date: "2024-01-01"     # 可选：从某天开始
  end_date: "2024-12-31"       # 可选：到某天结束
```

常用 Cron 表达式参考：
- `"0 6 * * *"` : 每天 06:00
- `"0 */4 * * *"` : 每 4 小时
- `"0 9 * * 1"` : 每周一 09:00

### 间隔触发

```yaml
schedule:
  type: interval
  interval_seconds: 300        # 每 5 分钟
```

### 手动触发（只能通过 CLI 或 UI 触发）

```yaml
schedule:
  type: manual
```

### 并发控制

```yaml
schedule:
  type: cron
  cron_expr: "*/5 * * * *"
  max_instances: 1             # 上一次未完成时，不启动新实例（默认 1）
```

---

## 数据流传递

Task 之间可以传递数据，上游的输出可以自动成为下游的输入。这在 ETL 流程中非常常见。

```yaml
tasks:
  - id: fetch_from_shopify
    connector: shopify
    action: fetch
    # fetch() 返回的数据被存入内部 TaskResult.output

  - id: transform
    connector: my_transformer
    action: process
    depends_on: [fetch_from_shopify]
    pass_output_from: fetch_from_shopify   # fetch_from_shopify 的输出
                                           # 会作为 data 参数传入这个 Task

  - id: write_to_db
    connector: postgres
    action: push
    depends_on: [transform]
    pass_output_from: transform            # transform 的输出传给 push(data=...)
```

对应的 Transformer Connector 里：

```python
class MyTransformer(BaseConnector):
    def process(self, data=None, **kwargs):  # data 参数接收来自上游的数据
        # 处理数据
        transformed = [clean(row) for row in data["orders"]]
        return transformed                  # 返回值成为下游的 data
```

---

## 重试与超时

你可以为每个 Task 单独配置重试策略和超时时间。

```yaml
tasks:
  - id: fetch_api
    retry:
      times: 3                 # 最多重试 3 次（不含第一次执行）
      delay_seconds: 10        # 第一次重试前等待 10 秒
      backoff: 2.0             # 指数退避：10s → 20s → 40s
    timeout_seconds: 120       # 超过 120 秒强制终止该任务
```

---

## 告警通知

框架支持基于配置的通知分发机制，支持防抖。

### 飞书 Webhook

内置了飞书通知器，配置方式如下：

```yaml
# pipelines/daily_sync.yaml
notify:
  on_task_failure: true
  on_pipeline_failure: true
  on_pipeline_success: false
  failure_threshold: 1          # 连续失败几次才发通知（防抖）
  channels:
    - name: feishu
      config:
        webhook_url: "${FEISHU_WEBHOOK_URL}"
```

### 框架内置的 LogOnlyNotifier

开发环境下如果不想配置 Webhook，可以使用内置的 log 通知器：

```yaml
notify:
  channels:
    - name: log                 # 内置，直接用，失败信息仅打印到 stdout
```

---

## Web UI 界面

安装了 `data-orchestrator[ui]` 后，框架自带基于 Streamlit 的监控大盘。使用 `orchestrator run` 启动时默认会拉起 UI，或单独启动：

```bash
orchestrator ui                 # 默认端口 8501
orchestrator ui --port 9000     # 自定义端口
```

UI 包含四个页面：

- **Dashboard**：今日总览。显示执行次数、成功率、耗时趋势图表；最近 10 次 Pipeline 执行状态；即将触发的任务列表。
- **Pipeline 管理**：运维操作。展示所有 Pipeline 的调度状态（运行中 / 已暂停）；提供手动触发、暂停、恢复按钮。
- **Run Detail (执行详情)**：点进某次执行查看详情。展示依赖关系 DAG 可视化图；每个 Task 的状态、耗时、重试次数；失败 Task 可展开查看完整错误堆栈。
- **Log Search (日志查询)**：历史检索。支持按 Pipeline / 状态 / 时间范围过滤；支持关键词搜索错误信息；分页展示并支持结果导出为 CSV。

---

## 命令行与 API 管理

Orchestrator 提供了丰富的 CLI 命令用于日常运维：

```bash
# 启动调度器并同时开启 UI
orchestrator run --config pipelines/ --plugins connectors/

# 无 UI 模式启动
orchestrator run --no-ui

# 仅启动 UI
orchestrator ui --port 9000

# 运维命令
orchestrator trigger daily_order_sync   # 手动立即触发
orchestrator list                       # 查看所有注册的 Pipeline
orchestrator status daily_order_sync    # 查看某个 Pipeline 的最近执行记录
orchestrator pause daily_order_sync     # 暂停调度
orchestrator resume daily_order_sync    # 恢复调度
orchestrator ping                       # 检查 Connector 连通性 (调用 ping)
orchestrator validate                   # 验证 YAML 配置合法性 (不运行)
```

### HTTP API

当 Orchestrator 启动后，后台会自动启动一个轻量级的 HTTP API（默认运行在 8765 端口）。你甚至可以将 `ORCHESTRATOR_API_URL` 提供给外部系统进行调度触发与管控：

- `GET /pipelines` - 列出所有流水线状态及下次执行时间
- `GET /pipeline/{id}` - 获取特定流水线的详细配置
- `GET /upcoming?hours=2` - 获取未来指定小时内即将触发的任务
- `POST /trigger/{id}` - 异步触发流水线，返回 run_id
- `POST /pause/{id}` - 暂停指定流水线
- `POST /resume/{id}` - 恢复指定流水线

---

## 内置 Connector

Orchestrator 内置了常见的通用 Connector，你可以直接在 YAML 中使用，无需额外编写代码：

### `postgres`

```yaml
connector: postgres
connector_config:
  dsn: "${DATABASE_URL}"
action: fetch
action_kwargs:
  query: "SELECT * FROM orders WHERE created_at >= :date"
  params:
    date: "{{ yesterday }}"
```

```yaml
action: push
action_kwargs:
  table: orders
  mode: upsert         # 支持 insert / upsert / replace / truncate_insert
  upsert_key: order_id
```

### `http_api`

```yaml
connector: http_api
connector_config:
  base_url: "https://api.example.com"
  headers:
    Authorization: "Bearer ${API_TOKEN}"
  rate_limit_rps: 2    # 每秒最多 2 个请求，自动限速
action: fetch
action_kwargs:
  endpoint: /v1/orders
  method: GET          # GET / POST / PUT / PATCH / DELETE
  params:
    status: paid
  timeout: 30
```

### `csv_file`

```yaml
connector: csv_file
connector_config:
  base_dir: "/data/exports"
action: fetch
action_kwargs:
  path: "orders_{{ today }}.csv"
  encoding: utf-8
```

```yaml
action: push
action_kwargs:
  path: "output_{{ today }}.csv"
  mode: overwrite      # overwrite / append
```

---

## 与现有方案对比

| | Orchestrator | Airflow | Prefect | crontab + 脚本 |
|---|---|---|---|---|
| **部署复杂度** | **极低** (单进程+SQLite 即可) | 重（Webserver+Scheduler+Worker+DB） | 需连接 Prefect Cloud 或自建 Server | 极简 |
| **自定义 Connector** | **极简** (几十行代码，任意方法名) | 需编写自定义 Operator，有框架抽象限制 | 需适配框架生态 | 随意但无复用 |
| **配置管理** | **YAML + 环境变量 + 动态渲染** | Python 脚本生成 DAG | Python 脚本 | 硬编码 |
| **重试/并发/超时** | ✅ 内置 | ✅ | ✅ | ❌ 自己写 |
| **可视化与监控** | ✅ 内置 Streamlit UI + HTTP API | ✅ 功能完整 | ✅ 功能完整 | ❌ |
| **适合团队规模** | **1-5 人 / 中小项目** | 10 人以上数据团队 | 中大型团队 | 1 人 |
| **学习成本** | **低** | 高 | 中 | 零 |

**适合用 Orchestrator 的场景：**
- 小团队或个人，不想维护 Airflow 这类重型基础设施
- 数据接口高度定制化，通用适配器成本高
- 想完全掌控执行逻辑，出了问题能追到每一行代码
- 已有自己的服务器，只需要一个 Python 进程跑起来

---

*MIT License*

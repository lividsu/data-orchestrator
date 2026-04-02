# Orchestrator

> 轻量级私有数据编排框架。把所有"定期去某个地方拿数据、处理、再放到某个地方"的事情，统一管理起来。

---

## 目录

- [为什么要用这个](#为什么要用这个)
- [核心概念](#核心概念)
- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [编写 Connector](#编写-connector)
- [编写 Pipeline（YAML）](#编写-pipelineyaml)
- [调度配置](#调度配置)
- [数据流传递](#数据流传递)
- [重试与超时](#重试与超时)
- [告警通知](#告警通知)
- [可视化 UI](#可视化-ui)
- [CLI 命令](#cli-命令)
- [内置 Connector](#内置-connector)
- [完整示例](#完整示例)
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

$ orchestrator run           ← 启动，之后不用管它

✓ 定时自动触发               ✓ 失败自动重试
✓ 飞书推送失败通知            ✓ Web UI 查看历史
────────────────────────────────────────────────────────────
```

---

## 核心概念

```
Connector   ── 知道怎么和某个系统交互。每个 Connector 可以定义任意 action（方法），
               比如 fetch / push / send_report / generate / transform …
               框架通过方法名调用，不限定你必须写哪些方法。
    ↓
Task        ── 用某个 Connector 做一件事（拉数据 / 推数据）
    ↓
Pipeline    ── 把多个 Task 按依赖顺序组合成一条链路
    ↓
Scheduler   ── 决定 Pipeline 什么时候跑
    ↓
UI / Log    ── 记录执行历史，展示状态
```

**核心设计原则：框架提供骨架，业务代码在框架外。**

框架只负责调度、重试、日志、通知这些通用能力。你的 Connector 实现、YAML 配置，完全放在自己的项目里，通过注册机制接入框架。升级框架不影响你的业务代码。

---

## 快速开始

### 安装

```bash
pip install data-orchestrator
```

### 最小可运行示例（5 分钟）

**第一步：写一个 Connector**

```python
# my_project/connectors/demo.py
from orchestrator import BaseConnector, register_connector

@register_connector("demo")
class DemoConnector(BaseConnector):
    """只需要实现你用到的方法。这里只用了 fetch，所以只写 fetch。"""

    def fetch(self, message="hello", **kwargs):
        print(f"[DemoConnector] fetching: {message}")
        return {"result": message, "count": 42}
```

**第二步：写一个 Pipeline**

```yaml
# my_project/pipelines/demo.yaml
pipelines:
  - id: demo_pipeline
    name: 演示 Pipeline
    schedule:
      type: interval
      interval_seconds: 10
    tasks:
      - id: fetch_demo
        connector: demo
        action: fetch
        action_kwargs:
          message: "第一次拉取"
```

**第三步：启动**

```python
# my_project/main.py
from orchestrator import Orchestrator
import my_project.connectors.demo  # import 触发注册

app = Orchestrator(config_dir="pipelines/")
app.start()
```

```bash
python main.py
# 或者用 CLI：
orchestrator run --config pipelines/ --plugins connectors/
```

打开 http://localhost:8501 查看 UI。

---

## 项目结构

框架本身和你的业务项目是**完全分离**的：

```
my_data_project/                 ← 你的项目（与框架无关）
├── connectors/                  ← 你写的 Connector
│   ├── __init__.py
│   ├── shopify.py
│   ├── feishu.py
│   └── postgres_custom.py
├── pipelines/                   ← YAML 配置
│   ├── daily_sync.yaml
│   ├── weekly_report.yaml
│   └── ad_monitor.yaml
├── .env                         ← 密钥和配置（不进 git）
└── main.py                      ← 3 行代码启动
```

框架安装在 site-packages，你的项目里不包含框架代码。

---

## 编写 Connector

所有 Connector 继承 `BaseConnector`，只需实现自己用到的方法。

框架通过 YAML 里的 `action` 字段按名称调用方法，因此 Connector 上的任何公开方法都可以作为 action。`fetch` / `push` / `ping` 是常见约定，但不是强制要求。

```python
from orchestrator import BaseConnector, register_connector

@register_connector("shopify")           # 注册名，YAML 里用这个名字引用
class ShopifyConnector(BaseConnector):

    def __init__(self, config: dict):
        super().__init__(config)
        self.shop_url = config["shop_url"]
        self.access_token = config["access_token"]
        # 做初始化，比如建立连接、创建 session

    def fetch(self, endpoint: str, params: dict = None, **kwargs):
        """
        拉取数据。
        返回值会被作为 TaskResult.output 传递给下游 Task。
        """
        url = f"{self.shop_url}/admin/api/2024-01/{endpoint}"
        headers = {"X-Shopify-Access-Token": self.access_token}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def push(self, data, endpoint: str, **kwargs):
        """
        推送数据。
        data 是上游 Task 的输出（如果有 depends_on 且配置了数据流）。
        """
        url = f"{self.shop_url}/admin/api/2024-01/{endpoint}"
        headers = {"X-Shopify-Access-Token": self.access_token}
        requests.post(url, headers=headers, json=data)

    def ping(self) -> bool:
        """
        连通性检查。
        返回 True/False，不要抛异常。
        框架会在启动时调用这个方法做健康检查。
        """
        try:
            url = f"{self.shop_url}/admin/api/2024-01/shop.json"
            headers = {"X-Shopify-Access-Token": self.access_token}
            r = requests.get(url, headers=headers, timeout=5)
            return r.status_code == 200
        except Exception:
            return False
```

### 自定义 Action

不是所有场景都适合 fetch / push 的语义。你可以定义任意方法名：

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

YAML 里用 `action: send_report`：

```yaml
tasks:
  - id: send_daily
    connector: feishu_daily
    action: send_report
    action_kwargs:
      title: "{{ today }} 日报"
      content: "数据同步完成"
```

### 让框架自动发现 Connector

不想在 `main.py` 里手动 import 每个 Connector 文件？用自动扫描：

```python
# main.py
from orchestrator import Orchestrator

app = Orchestrator(
    plugin_dir="connectors/",    # 框架启动时自动 import 这个目录下所有 .py 文件
    config_dir="pipelines/",
)
app.start()
```

### Connector 配置安全

Connector 的敏感配置（token、密码）写在 `.env`，YAML 里用 `${}` 引用：

```yaml
connector_config:
  shop_url: "${SHOPIFY_SHOP_URL}"
  access_token: "${SHOPIFY_ACCESS_TOKEN}"
```

---

## 编写 Pipeline（YAML）

### 完整字段说明

```yaml
pipelines:
  - id: daily_order_sync                 # 唯一 ID，CLI 触发时用
    name: 每日订单同步                    # 可读名称，显示在 UI 里
    description: 从 Shopify 拉取昨日订单，写入 Postgres

    schedule:                            # 调度配置，见下方"调度配置"章节
      type: cron
      cron_expr: "0 6 * * *"
      timezone: "Asia/Shanghai"

    max_concurrency: 4                   # 无依赖的 Task 最多并发几个
    stop_on_failure: true                # 某个 Task 失败后，是否停止后续 Task

    notify:                              # 通知配置，见下方"告警通知"章节
      on_failure: true
      on_success: false
      channels: [feishu]

    tasks:
      - id: fetch_orders                 # Task ID，depends_on 里引用这个
        name: 拉取 Shopify 订单
        connector: shopify               # 对应 @register_connector("shopify")
        connector_config:
          shop_url: "${SHOPIFY_URL}"
          access_token: "${SHOPIFY_TOKEN}"
        action: fetch                    # Connector 上的任意方法名
        action_kwargs:                   # 传给 connector.fetch() 的参数
          endpoint: orders.json
          params:
            status: paid
            created_at_min: "{{ yesterday_iso }}"  # 内置变量
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
          mode: upsert                   # insert / upsert / replace
          upsert_key: order_id
        retry:
          times: 2
          delay_seconds: 5

      - id: notify_feishu
        name: 发送完成通知
        connector: feishu
        connector_config:
          webhook_url: "${FEISHU_WEBHOOK}"
        action: push
        depends_on: [write_to_postgres]
        action_kwargs:
          template: order_sync_done      # 用 feishu connector 内部定义的模板名
```

### 内置模板变量

在 `action_kwargs` 里可以使用这些变量，框架在执行时自动替换：

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

```
"0 6 * * *"        每天 06:00
"0 */4 * * *"      每 4 小时
"0 9 * * 1"        每周一 09:00
"0 8 1 * *"        每月 1 号 08:00
"*/30 * * * *"     每 30 分钟
```

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

Task 之间可以传递数据，上游的输出自动成为下游的输入：

```yaml
tasks:
  - id: fetch_from_shopify
    connector: shopify
    action: fetch
    # fetch() 返回的数据被存入 TaskResult.output

  - id: transform
    connector: my_transformer
    action: fetch
    depends_on: [fetch_from_shopify]
    pass_output_from: fetch_from_shopify   # fetch_from_shopify 的输出
                                           # 会作为 data 参数传入这个 Task

  - id: write_to_db
    connector: postgres
    action: push
    depends_on: [transform]
    pass_output_from: transform            # transform 的输出传给 push(data=...)
```

对应的 Connector 里：

```python
class MyTransformer(BaseConnector):
    def fetch(self, data=None, **kwargs):  # data 来自上游
        # 处理数据
        transformed = [clean(row) for row in data["orders"]]
        return transformed                  # 返回值成为下游的 data
```

---

## 重试与超时

### Task 级别配置

```yaml
tasks:
  - id: fetch_api
    retry:
      times: 3                 # 最多重试 3 次（不含第一次）
      delay_seconds: 10        # 第一次重试等待 10 秒
      backoff: 2.0             # 指数退避：10s → 20s → 40s
      on_exceptions:           # 可选：只对哪些异常重试
        - requests.Timeout
        - requests.ConnectionError
    timeout_seconds: 120       # 超过 120 秒强制终止
```

### 全局默认值（在 Settings 里配置）

```yaml
# config/settings.yaml
defaults:
  retry:
    times: 3
    delay_seconds: 5
    backoff: 2.0
  timeout_seconds: 60
```

Task 级别的配置会覆盖全局默认值。

---

## 告警通知

### 飞书 Webhook

```python
# my_project/connectors/feishu_notifier.py
from orchestrator import BaseNotifier, register_notifier

@register_notifier("feishu")
class FeishuNotifier(BaseNotifier):
    def send(self, title: str, body: str, level: str, context: dict):
        # level: "info" / "warning" / "error"
        # context: pipeline_id, task_id, error_message, run_id 等
        webhook_url = self.config["webhook_url"]
        payload = {
            "msg_type": "interactive",
            "card": self._build_card(title, body, level, context)
        }
        requests.post(webhook_url, json=payload)

    def _build_card(self, title, body, level, context):
        # 自定义飞书卡片格式
        ...
```

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

开发环境下不想配置 webhook，用内置的 log 通知器：

```yaml
notify:
  channels:
    - name: log                 # 内置，直接用，失败信息打印到 stdout
```

---

## 可视化 UI

框架内置 Streamlit UI，启动后访问 http://localhost:8501：

```bash
orchestrator ui                 # 默认端口 8501
orchestrator ui --port 9000     # 自定义端口
```

UI 包含四个页面：

**Dashboard** — 今日总览
- 执行次数、成功率、平均耗时
- 最近 10 次 Pipeline 执行状态
- 即将触发的任务列表

**Pipeline 详情** — 点进某次执行
- 每个 Task 的状态、耗时、重试次数
- 失败 Task 展开查看完整错误堆栈
- 依赖关系可视化

**Pipeline 管理** — 运维操作
- 所有 Pipeline 的调度状态（运行中 / 已暂停）
- 手动触发按钮
- 暂停 / 恢复

**日志查询** — 历史检索
- 按 Pipeline / 状态 / 时间范围过滤
- 关键词搜索错误信息
- 分页展示

---

## CLI 命令

```bash
# 启动调度器（持续运行）
orchestrator run
orchestrator run --config pipelines/         # 指定配置目录
orchestrator run --plugins connectors/       # 指定插件目录

# 手动立即触发某个 Pipeline
orchestrator trigger daily_order_sync

# 验证 YAML 配置（不运行）
orchestrator validate --config pipelines/daily_sync.yaml

# 查看所有注册的 Pipeline
orchestrator list

# 查看某个 Pipeline 的状态和最近执行历史
orchestrator status daily_order_sync

# 暂停 / 恢复
orchestrator pause daily_order_sync
orchestrator resume daily_order_sync

# 检查 Connector 连通性（调用 ping()，未实现 ping 的 connector 会显示 "not implemented"）
orchestrator ping

# 启动 UI
orchestrator ui
orchestrator ui --port 9000
```

---

## 内置 Connector

框架内置了常用的通用 Connector，可以直接在 YAML 里用：

### `postgres`

```yaml
connector: postgres
connector_config:
  dsn: "${DATABASE_URL}"
  # 或者分开写：
  host: localhost
  port: 5432
  dbname: mydb
  user: "${DB_USER}"
  password: "${DB_PASSWORD}"
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
  mode: upsert         # insert / upsert / replace / truncate_insert
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

## 完整示例

仓库内置的可运行示例目录：

- `examples/01_minimal/`：最小示例（对应快速开始）
- `examples/02_shopify_to_postgres/`：Shopify 到 Postgres 的真实场景
- `examples/03_parallel_tasks/`：DAG 分层并发执行演示
- `examples/04_custom_action/`：飞书日报推送（自定义 Action）

### 示例 1：每日从 Shopify 同步订单到 Postgres

```yaml
# pipelines/shopify_daily.yaml
pipelines:
  - id: shopify_daily_sync
    name: Shopify 每日订单同步
    schedule:
      type: cron
      cron_expr: "0 7 * * *"
    stop_on_failure: true
    notify:
      on_failure: true
      channels:
        - name: feishu
          config:
            webhook_url: "${FEISHU_OPS_WEBHOOK}"
    tasks:
      - id: fetch_shopify_orders
        connector: shopify
        connector_config:
          shop_url: "${SHOPIFY_URL}"
          access_token: "${SHOPIFY_TOKEN}"
        action: fetch
        action_kwargs:
          endpoint: orders.json
          params:
            status: paid
            created_at_min: "{{ yesterday_iso }}"
        retry:
          times: 3
          delay_seconds: 10

      - id: write_orders
        connector: postgres
        connector_config:
          dsn: "${DATABASE_URL}"
        action: push
        depends_on: [fetch_shopify_orders]
        pass_output_from: fetch_shopify_orders
        action_kwargs:
          table: raw_shopify_orders
          mode: upsert
          upsert_key: id
```

### 示例 2：每小时广告数据监控（无依赖并发）

```yaml
# pipelines/ad_monitor.yaml
pipelines:
  - id: ad_monitor_hourly
    name: 广告数据每小时监控
    schedule:
      type: cron
      cron_expr: "0 * * * *"
    max_concurrency: 3           # 三个拉取任务并发执行
    tasks:
      - id: fetch_sp_ads
        connector: amazon_sp_api
        connector_config:
          marketplace: US
          refresh_token: "${SP_API_REFRESH_TOKEN}"
        action: fetch
        action_kwargs:
          report_type: spAdvertising
          date: "{{ today }}"

      - id: fetch_sb_ads          # 和 fetch_sp_ads 并发执行（无依赖关系）
        connector: amazon_sp_api
        connector_config:
          marketplace: US
          refresh_token: "${SP_API_REFRESH_TOKEN}"
        action: fetch
        action_kwargs:
          report_type: sbAdvertising
          date: "{{ today }}"

      - id: fetch_sd_ads          # 同上，并发
        connector: amazon_sp_api
        connector_config:
          marketplace: US
          refresh_token: "${SP_API_REFRESH_TOKEN}"
        action: fetch
        action_kwargs:
          report_type: sdAdvertising
          date: "{{ today }}"

      - id: merge_and_write       # 等上面三个都完成后执行
        connector: postgres
        connector_config:
          dsn: "${DATABASE_URL}"
        action: push
        depends_on: [fetch_sp_ads, fetch_sb_ads, fetch_sd_ads]
        action_kwargs:
          table: ad_performance
          mode: upsert
          upsert_key: [date, ad_type, campaign_id]
```

### 示例 3：新增一个 Shopify Connector（完整步骤）

```python
# my_project/connectors/shopify.py

import requests
from orchestrator import BaseConnector, register_connector


@register_connector("shopify")
class ShopifyConnector(BaseConnector):

    def __init__(self, config: dict):
        super().__init__(config)
        self.shop_url = config["shop_url"].rstrip("/")
        self.access_token = config["access_token"]
        self.api_version = config.get("api_version", "2024-01")
        self._session = requests.Session()
        self._session.headers.update({
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        })

    def _url(self, endpoint: str) -> str:
        return f"{self.shop_url}/admin/api/{self.api_version}/{endpoint}"

    def fetch(self, endpoint: str, params: dict = None, **kwargs):
        response = self._session.get(self._url(endpoint), params=params)
        response.raise_for_status()
        return response.json()

    def push(self, data, endpoint: str, method: str = "POST", **kwargs):
        func = getattr(self._session, method.lower())
        response = func(self._url(endpoint), json=data)
        response.raise_for_status()
        return response.json()

    def ping(self) -> bool:
        try:
            r = self._session.get(self._url("shop.json"), timeout=5)
            return r.status_code == 200
        except Exception:
            return False
```

```python
# my_project/main.py

from orchestrator import Orchestrator

app = Orchestrator(
    plugin_dir="connectors/",       # 自动扫描并注册所有 connector
    config_dir="pipelines/",        # 加载所有 pipeline yaml
    db_url="sqlite:///orchestrator.db",
)
app.start()
```

```bash
$ python main.py
[Orchestrator] Loading plugins from connectors/...
[Orchestrator] Registered connector: shopify
[Orchestrator] Registered connector: feishu
[Orchestrator] Loaded pipeline: shopify_daily_sync (next run: 2024-03-16 07:00:00)
[Orchestrator] Loaded pipeline: ad_monitor_hourly (next run: 2024-03-15 11:00:00)
[Orchestrator] UI available at http://localhost:8501
[Orchestrator] Started. Press Ctrl+C to stop.
```

### 示例 4：飞书日报推送（自定义 Action）

```python
# connectors/feishu_daily.py
import requests
from orchestrator import BaseConnector, register_connector

@register_connector("feishu_daily")
class FeishuDailyConnector(BaseConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.webhook_url = config["webhook_url"]

    def send_report(self, title="日报", content="", **kwargs):
        resp = requests.post(
            self.webhook_url,
            json={"msg_type": "text", "content": {"text": f"{title}\n{content}"}},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
```

```yaml
# pipelines/daily_report.yaml
pipelines:
  - id: feishu_daily_report
    name: 飞书日报
    schedule:
      type: cron
      cron_expr: "0 18 * * 1-5"
    tasks:
      - id: send_daily
        connector: feishu_daily
        connector_config:
          webhook_url: "${FEISHU_DAILY_WEBHOOK}"
        action: send_report
        action_kwargs:
          title: "{{ today }} 日报"
          content: "今日数据同步正常"
```

---

## 与现有方案对比

| | Orchestrator | Airflow | Prefect | crontab + 脚本 |
|---|---|---|---|---|
| 部署复杂度 | 一个 Python 进程 | 重（Webserver + Scheduler + Worker + DB） | 需连接 Prefect Cloud 或自建 | 极简 |
| 自定义 Connector | 50 行，完全自由 | 自定义 Operator，有框架限制 | 有约束 | 随意但无复用 |
| 重试 / 超时 | ✅ 内置 | ✅ | ✅ | ❌ 自己写 |
| 可视化 | ✅ Streamlit UI | ✅ 功能完整 | ✅ 功能完整 | ❌ |
| 失败通知 | ✅ 可插拔 | ✅ | ✅ | ❌ |
| 适合团队规模 | 1-5 人 | 10 人以上 | 中大型 | 1 人 |
| 可控性 | 完全掌控 | 黑盒较多 | 依赖第三方 | 完全掌控 |
| 学习成本 | 低 | 高 | 中 | 零 |

**适合用 Orchestrator 的场景：**
- 小团队或个人，不想维护 Airflow 这类重型基础设施
- 数据接口高度定制化，通用适配器成本高
- 想完全掌控执行逻辑，出了问题能追到每一行代码
- 已有自己的服务器，只需要一个 Python 进程跑起来

---

## Contributing：新增 Connector 规范

添加一个新 Connector 的标准步骤：

1. 在 `connectors/` 创建 `{name}.py`
2. 继承 `BaseConnector`，实现你需要的方法（可以是 `fetch` / `push` / `ping`，也可以是任意自定义方法）
3. 加 `@register_connector("{name}")` 装饰器
4. 在 `tests/connectors/test_{name}.py` 写测试（覆盖实际实现的方法和错误路径）
5. 在 `examples/` 添加对应的 YAML 示例

**Connector 实现要求：**
- 只实现你用到的方法，不需要补空的 `fetch` / `push` / `ping`
- 如果实现了 `ping()`，必须捕获所有异常并返回 `True` / `False`
- 如果实现了 `fetch()`，失败时抛出异常（框架负责重试）
- 如果实现了 `push()`，要考虑幂等（重复执行不产生重复数据）
- 自定义 action 方法签名建议接受 `**kwargs`，以便框架传入上游数据
- 不在 `__init__` 里做耗时操作，连接池/session 可以懒初始化

---

*MIT License*

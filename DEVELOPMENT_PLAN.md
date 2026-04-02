# 开发计划：Connector 方法可选化

> 目标：将 `BaseConnector` 的 `fetch` / `push` / `ping` 从强制实现改为可选，支持任意自定义 action。不保持向后兼容。

---

## 背景

当前 `BaseConnector` 将 `fetch`、`push`、`ping` 标记为 `@abstractmethod`，要求所有子类必须实现全部三个方法。但实际场景中，很多 connector 只需要其中一部分能力：

- 飞书日报推送：只需要一个 `send_report` 方法，不属于 fetch 也不属于 push
- 只读数据源：只需要 `fetch`，永远不会 `push`
- 健康检查无意义的场景：比如本地文件生成器，`ping` 没有语义

Runner 层（`runner.py:114`）已经通过 `getattr(connector, task.action)` 动态调用，天然支持任意 action 名。限制仅在 `BaseConnector` 的 ABC 定义上。

---

## 涉及文件清单

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `orchestrator/connectors/base.py` | **重写** | 移除 `@abstractmethod`，提供默认 `NotImplementedError` 实现 |
| `orchestrator/core/scheduler.py` | **修改** | `ping_all` 对 `NotImplementedError` 做容错 |
| `orchestrator/cli.py` | **修改** | `ping` 命令显示适配 + `init` 模板注释更新 |
| `orchestrator/core/runner.py` | **修改** | `execute()` 增加 action 不存在时的清晰报错 |
| `tests/conftest.py` | **修改** | `MockConnector` 简化 |
| `tests/connectors/test_registry.py` | **修改** | 移除/替换 abstractmethod 相关测试 |
| `tests/core/test_task_runner.py` | **修改** | 测试 connector 内联类简化 + 新增自定义 action 测试 |
| `tests/connectors/test_custom_action.py` | **新增** | 自定义 action connector 专项测试 |
| `examples/04_custom_action/` | **新增** | 飞书日报等自定义 action 示例 |
| `README.md` | **重写相关章节** | 更新文档反映新设计 |
| `CONTRIBUTING.md` | **重写** | 更新 Connector 编写规范 |

---

## 第一阶段：核心改动

### 1.1 重写 `orchestrator/connectors/base.py`

```python
from __future__ import annotations

import asyncio
from abc import ABC
from typing import Any


class BaseConnector(ABC):
    """所有 Connector 的基类。

    子类只需实现自己用到的方法。Runner 通过 getattr 按名称调用 action，
    因此 connector 上的任何公开方法都可以作为 YAML 里的 action 使用。

    内置的 fetch / push / ping 是常见约定，但不是强制要求。
    """

    def __init__(self, config: dict):
        self.config = config
        self._initialized = False

    def initialize(self):
        self._initialized = True

    def fetch(self, **kwargs: Any) -> Any:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement 'fetch'."
        )

    def push(self, data: Any = None, **kwargs: Any) -> None:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement 'push'."
        )

    def ping(self) -> bool:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement 'ping'."
        )

    def close(self):
        pass

    async def async_fetch(self, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self.fetch, **kwargs)

    async def async_push(self, data: Any = None, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self.push, data, **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(config_keys={list(self.config.keys())})"
```

**关键变化：**

- 移除 `from abc import abstractmethod`，只保留 `ABC`（保留 ABC 是为了防止直接实例化 `BaseConnector` 本身——但因为没有 abstractmethod 了，ABC 实际上不再阻止实例化，需要视情况决定是否保留，见下方说明）
- `fetch` / `push` / `ping` 变为普通方法，默认抛 `NotImplementedError`
- 新增 docstring 明确说明设计意图

**关于是否保留 ABC 基类：**

移除 `@abstractmethod` 后，`ABC` 不再阻止直接实例化 `BaseConnector`。有两个选择：

- **方案 A：去掉 ABC**，改为普通 class。简单直接。
- **方案 B：保留 ABC**，作为语义标记，告诉开发者"这是基类，不要直接实例化"。

推荐**方案 A**，因为 ABC 此时没有实际约束力，保留反而容易误导。如果要防止直接实例化，可以在 `__init_subclass__` 或 registry 层面做检查。

### 1.2 修改 `orchestrator/core/runner.py`

在 `TaskRunner.execute()` 里增加 action 不存在时的清晰报错：

```python
# runner.py 第 113-117 行区域，改为：
def execute() -> Any:
    action_fn = getattr(connector, task.action, None)
    if action_fn is None or not callable(action_fn):
        raise AttributeError(
            f"Connector '{task.connector_name}' does not have action '{task.action}'. "
            f"Available actions: {[m for m in dir(connector) if not m.startswith('_') and callable(getattr(connector, m))]}"
        )
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(action_fn, **kwargs)
        return future.result(timeout=task.timeout_seconds)
```

这样当 YAML 里写了 `action: send_report` 但 connector 没有这个方法时，用户能看到清晰的报错，包含可用方法列表。

### 1.3 修改 `orchestrator/core/scheduler.py` — `ping_all`

```python
def ping_all(self) -> dict[str, bool | None]:
    """返回值含义：True=健康 / False=不健康 / None=未实现 ping"""
    # ... 省略前面不变的部分 ...
    for connector_name, connector_config in connector_inputs.values():
        result_by_connector.setdefault(connector_name, [])
        connector = None
        try:
            connector = get_connector(connector_name, connector_config)
            connector.initialize()
            result_by_connector[connector_name].append(bool(connector.ping()))
        except NotImplementedError:
            result_by_connector[connector_name].append(None)  # 未实现
        except Exception:
            result_by_connector[connector_name].append(False)  # 实际失败
        finally:
            if connector is not None:
                connector.close()

    def _aggregate(results: list[bool | None]) -> bool | None:
        bools = [r for r in results if r is not None]
        if not bools:
            return None
        return all(bools)

    return {name: _aggregate(results) for name, results in result_by_connector.items()}
```

### 1.4 修改 `orchestrator/cli.py`

**ping 命令输出适配：**

```python
for name, ok in result.items():
    if ok is None:
        status = "⏭️  not implemented"
    elif ok:
        status = "✅"
    else:
        status = "❌"
    table.add_row(name, status)
```

**init 模板更新：**

生成的 demo connector 加注释说明方法可选，并展示自定义 action 的写法：

```python
@register_connector('demo')
class DemoConnector(BaseConnector):
    # fetch / push / ping 都是可选的。
    # 任何公开方法都可以作为 YAML 里的 action 使用。

    def fetch(self, **kwargs):
        return {'message': 'hello from demo connector'}

    def push(self, data=None, **kwargs):
        return {'ok': True, 'received': data}

    def ping(self):
        return True
```

---

## 第二阶段：测试更新

### 2.1 修改 `tests/conftest.py`

`MockConnector` 不再需要实现所有三个方法，简化为只保留测试实际需要的：

```python
class MockConnector(BaseConnector):
    def fetch(self, **kwargs):
        return {"ok": True, "kwargs": kwargs}

    def push(self, data, **kwargs):
        return {"ok": True, "data": data, "kwargs": kwargs}

    def ping(self) -> bool:
        return True
```

> 这个文件实际不需要改，但需要确认它还能正常工作。因为不再有 abstractmethod，即使只实现一个方法也不会报错。

### 2.2 修改 `tests/connectors/test_registry.py`

**删除 `test_cannot_instantiate_abstract_class` 测试。**

原测试验证 `BaseConnector({})` 会抛 `TypeError`。移除 abstractmethod 后此行为不再存在。替换为：

```python
def test_base_connector_methods_raise_not_implemented():
    """直接调用 BaseConnector 的 fetch/push/ping 应该抛出 NotImplementedError。"""
    conn = BaseConnector({})
    with pytest.raises(NotImplementedError):
        conn.fetch()
    with pytest.raises(NotImplementedError):
        conn.push()
    with pytest.raises(NotImplementedError):
        conn.ping()
```

**简化其他测试中的 MockConnector 定义。** 当前每个测试内联的 MockConnector 都实现了全部三个方法，虽然只测 fetch。可以去掉不需要的方法：

```python
# 以前（每个测试都写三个方法）
@register_connector("test_mock")
class MockConnector(BaseConnector):
    def fetch(self, **kwargs):
        return {"data": 1}
    def push(self, data=None, **kwargs):  # 没用到
        return None
    def ping(self):                       # 没用到
        return True

# 改为
@register_connector("test_mock")
class MockConnector(BaseConnector):
    def fetch(self, **kwargs):
        return {"data": 1}
```

### 2.3 修改 `tests/core/test_task_runner.py`

同上，每个测试内联 connector 去掉未使用的方法。额外新增测试：

```python
def test_custom_action():
    """connector 可以定义任意方法作为 action。"""
    @register_connector("custom_action_connector")
    class CustomActionConnector(BaseConnector):
        def send_report(self, content="", **kwargs):
            return {"sent": True, "content": content}

    task = Task(
        id="t_custom",
        connector="custom_action_connector",
        action="send_report",
        action_kwargs={"content": "日报内容"},
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.SUCCESS
    assert result.output == {"sent": True, "content": "日报内容"}


def test_missing_action_gives_clear_error():
    """action 不存在时应给出清晰报错，包含可用方法列表。"""
    @register_connector("no_such_action_connector")
    class NoSuchActionConnector(BaseConnector):
        def fetch(self, **kwargs):
            return {}

    task = Task(
        id="t_missing",
        connector="no_such_action_connector",
        action="nonexistent_action",
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.FAILED
    assert "nonexistent_action" in result.error_message
    assert "fetch" in result.error_message  # 提示可用方法


def test_unimplemented_default_action_gives_not_implemented():
    """调用未覆写的默认方法（如 push）应该抛 NotImplementedError。"""
    @register_connector("fetch_only_connector")
    class FetchOnlyConnector(BaseConnector):
        def fetch(self, **kwargs):
            return {"ok": True}

    task = Task(
        id="t_no_push",
        connector="fetch_only_connector",
        action="push",
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.FAILED
    assert "NotImplementedError" in result.error_type
```

### 2.4 新增 `tests/connectors/test_custom_action.py`

自定义 action 场景的专项测试：

```python
"""测试只实现部分方法或自定义 action 的 connector。"""

import pytest
from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector
from orchestrator.connectors import registry


@pytest.fixture(autouse=True)
def clear_registry():
    registry._REGISTRY.clear()
    yield
    registry._REGISTRY.clear()


def test_fetch_only_connector():
    @register_connector("fetch_only")
    class FetchOnly(BaseConnector):
        def fetch(self, **kwargs):
            return [1, 2, 3]

    conn = FetchOnly(config={})
    assert conn.fetch() == [1, 2, 3]

    with pytest.raises(NotImplementedError):
        conn.push()
    with pytest.raises(NotImplementedError):
        conn.ping()


def test_push_only_connector():
    @register_connector("push_only")
    class PushOnly(BaseConnector):
        def push(self, data=None, **kwargs):
            return {"sent": True}

    conn = PushOnly(config={})
    assert conn.push(data={"x": 1}) == {"sent": True}

    with pytest.raises(NotImplementedError):
        conn.fetch()


def test_fully_custom_action_connector():
    @register_connector("feishu_notifier")
    class FeishuNotifier(BaseConnector):
        def send_report(self, content: str = "", channel: str = "", **kwargs):
            return {"ok": True, "channel": channel, "content": content}

        def send_alert(self, message: str = "", **kwargs):
            return {"ok": True, "message": message}

    conn = FeishuNotifier(config={"webhook_url": "https://example.com"})
    result = conn.send_report(content="日报", channel="#daily")
    assert result == {"ok": True, "channel": "#daily", "content": "日报"}

    # fetch / push / ping 都未实现
    with pytest.raises(NotImplementedError):
        conn.fetch()
    with pytest.raises(NotImplementedError):
        conn.push()
    with pytest.raises(NotImplementedError):
        conn.ping()
```

---

## 第三阶段：新增示例

### 3.1 新增 `examples/04_custom_action/`

目录结构：

```
examples/04_custom_action/
├── connectors/
│   └── feishu_daily.py
├── pipelines/
│   └── daily_report.yaml
└── main.py
```

**`connectors/feishu_daily.py`：**

```python
from __future__ import annotations
import requests
from orchestrator import BaseConnector, register_connector


@register_connector("feishu_daily")
class FeishuDailyConnector(BaseConnector):
    """
    只做一件事：发送日报到飞书。
    不需要 fetch / push / ping，只定义 send_report。
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.webhook_url = config["webhook_url"]

    def send_report(self, title: str = "日报", content: str = "", **kwargs):
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {"title": {"tag": "plain_text", "content": title}},
                "elements": [
                    {"tag": "markdown", "content": content}
                ],
            },
        }
        resp = requests.post(self.webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
```

**`pipelines/daily_report.yaml`：**

```yaml
pipelines:
  - id: feishu_daily_report
    name: 每日飞书日报
    schedule:
      type: cron
      cron_expr: "0 18 * * 1-5"
      timezone: "Asia/Shanghai"
    tasks:
      - id: send_daily
        connector: feishu_daily
        connector_config:
          webhook_url: "${FEISHU_DAILY_WEBHOOK}"
        action: send_report          # 自定义 action，不是 fetch 也不是 push
        action_kwargs:
          title: "{{ today }} 日报"
          content: |
            **今日完成：**
            - 数据同步正常
            - 报表生成完毕
```

**`main.py`：**

```python
from orchestrator import Orchestrator

app = Orchestrator(
    plugin_dir="connectors/",
    config_dir="pipelines/",
)
app.start()
```

---

## 第四阶段：文档更新

### 4.1 README.md 修改清单

以下按 README 章节顺序列出需要修改的部分。

---

#### 4.1.1 「核心概念」章节

**原文：**

```
Connector   ── 知道怎么和某个系统通信（Shopify、Postgres、飞书…）
```

**改为：**

```
Connector   ── 知道怎么和某个系统交互。每个 Connector 可以定义任意 action（方法），
               比如 fetch / push / send_report / generate / transform …
               框架通过方法名调用，不限定你必须写哪些方法。
```

---

#### 4.1.2 「快速开始 > 第一步：写一个 Connector」章节

**原文：**

```python
@register_connector("demo")
class DemoConnector(BaseConnector):
    def fetch(self, message="hello", **kwargs):
        print(f"[DemoConnector] fetching: {message}")
        return {"result": message, "count": 42}

    def push(self, data, **kwargs):
        print(f"[DemoConnector] pushing: {data}")

    def ping(self):
        return True
```

**改为：**

```python
@register_connector("demo")
class DemoConnector(BaseConnector):
    """只需要实现你用到的方法。这里只用了 fetch，所以只写 fetch。"""

    def fetch(self, message="hello", **kwargs):
        print(f"[DemoConnector] fetching: {message}")
        return {"result": message, "count": 42}
```

---

#### 4.1.3 「编写 Connector」章节

**原文第一句：**

> 所有 Connector 继承 `BaseConnector`，实现三个方法：

**改为：**

> 所有 Connector 继承 `BaseConnector`，只需实现自己用到的方法。
>
> 框架通过 YAML 里的 `action` 字段按名称调用方法，因此 Connector 上的任何公开方法都可以作为 action。
> `fetch` / `push` / `ping` 是常见约定，但不是强制要求。

**在 Shopify 示例之后，新增一个「自定义 Action」小节：**

````markdown
### 自定义 Action

不是所有场景都适合 fetch / push 的语义。你可以定义任意方法名：

```python
@register_connector("feishu_daily")
class FeishuDailyConnector(BaseConnector):
    """这个 connector 只做一件事：发日报。不需要 fetch / push / ping。"""

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
````

---

#### 4.1.4 「编写 Pipeline（YAML）> 完整字段说明」中的 action 字段

**原文：**

```yaml
        action: fetch                    # fetch 或 push
```

**改为：**

```yaml
        action: fetch                    # Connector 上的任意方法名
```

---

#### 4.1.5 「CLI 命令」中 `orchestrator ping` 的说明

**原文：**

```
# 检查所有 Connector 的连通性（调用 ping()）
orchestrator ping
```

**改为：**

```
# 检查 Connector 连通性（调用 ping()，未实现 ping 的 connector 会显示 "not implemented"）
orchestrator ping
```

---

#### 4.1.6 「完整示例」新增示例 4

在现有三个示例之后，新增：

````markdown
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
        resp = requests.post(self.webhook_url, json={
            "msg_type": "text",
            "content": {"text": f"{title}\n{content}"}
        }, timeout=10)
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
````

---

#### 4.1.7 「Contributing：新增 Connector 规范」章节

**原文：**

> 1. 在 `connectors/` 创建 `{name}.py`
> 2. 继承 `BaseConnector`，实现 `fetch` / `push` / `ping`
> 3. 加 `@register_connector("{name}")` 装饰器

**改为：**

> 1. 在 `connectors/` 创建 `{name}.py`
> 2. 继承 `BaseConnector`，实现你需要的方法（可以是 `fetch` / `push` / `ping`，也可以是任意自定义方法）
> 3. 加 `@register_connector("{name}")` 装饰器

**原文 Connector 实现要求：**

> - `ping()` 必须捕获所有异常，返回 True/False，不抛出
> - `fetch()` 失败时抛出异常（框架负责重试）
> - `push()` 要考虑幂等（重复执行不产生重复数据）

**改为：**

> - 只实现你用到的方法，不需要补空的 `fetch` / `push` / `ping`
> - 如果实现了 `ping()`，必须捕获所有异常并返回 `True` / `False`
> - 如果实现了 `fetch()`，失败时抛出异常（框架负责重试）
> - 如果实现了 `push()`，要考虑幂等（重复执行不产生重复数据）
> - 自定义 action 方法签名建议接受 `**kwargs`，以便框架传入上游数据

---

### 4.2 CONTRIBUTING.md 完整重写

```markdown
# Contributing Guide

## 如何添加新 Connector

1. 在 `orchestrator/connectors/` 或你的项目插件目录新建文件
2. 继承 `BaseConnector`，只实现你需要的方法
3. 使用 `@register_connector("name")` 完成注册
4. 在 `tests/connectors/test_{name}.py` 增加测试
5. 在 `examples/` 增加对应示例

## Connector 实现规范

- **只写你需要的方法。** 不需要补空的 fetch / push / ping
- 如果实现了 `ping()`：必须捕获所有异常并返回布尔值
- 如果实现了 `push()`：必须考虑幂等行为
- 自定义 action 方法建议接受 `**kwargs`
- 不要在 `__init__` 里做耗时网络调用
- 配置字段缺失时抛出清晰 `ValueError`

## 测试规范

- Connector 测试文件路径统一为 `tests/connectors/test_{name}.py`
- 每个 Connector 只需覆盖它实际实现的方法：
  - 各 action 方法的成功路径
  - 错误处理路径
  - 如果实现了 `ping`：测试成功和失败
  - 未实现的方法不需要测试
```

---

## 执行顺序 Checklist

```
Phase 1 — 核心
  [ ] 1.1  重写 base.py，移除 @abstractmethod
  [ ] 1.2  修改 runner.py，增加 action 不存在时的报错
  [ ] 1.3  修改 scheduler.py，ping_all 容错 NotImplementedError
  [ ] 1.4  修改 cli.py，ping 输出 + init 模板

Phase 2 — 测试
  [ ] 2.1  确认 conftest.py MockConnector 正常
  [ ] 2.2  修改 test_registry.py，替换 abstract 测试
  [ ] 2.3  修改 test_task_runner.py，简化 + 新增自定义 action 测试
  [ ] 2.4  新增 test_custom_action.py

Phase 3 — 示例
  [ ] 3.1  新增 examples/04_custom_action/

Phase 4 — 文档
  [ ] 4.1  更新 README.md（7 处修改）
  [ ] 4.2  重写 CONTRIBUTING.md

Final
  [ ] 全量运行 pytest，确保通过
  [ ] 手动测试 CLI：orchestrator ping / orchestrator init
```

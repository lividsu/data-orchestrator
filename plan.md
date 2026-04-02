# data-orchestrator 深度优化计划

> 基于对仓库全部源码、测试、示例、配置的逐行审阅，整理出以下优化方向。
> 按 **紧急程度 × 影响面** 排列，分为 5 个大类、18 个具体条目。

---

## 一、仓库卫生与工程规范（立即修复）

### 1.1 清理不应提交到 Git 的文件

**现状**：仓库中包含了 `.coverage`、`orchestrator.db`（32KB 的 SQLite 数据库）、`data_orchestrator.egg-info/` 目录、以及 `__pycache__/` 编译缓存。这些都是构建/运行产物，不应进入版本控制。

**修复方案**：
- 创建 `.gitignore`，至少包含：`*.pyc`、`__pycache__/`、`*.egg-info/`、`.coverage`、`*.db`、`.env`、`dist/`、`build/`、`logs/`
- 用 `git rm --cached` 清理已提交的产物
- 注意 `examples/shopify_daily/connectors/__pycache__/` 也被提交了

### 1.2 缺少 `__version__` 导出

**现状**：`pyproject.toml` 定义了 `version = "0.1.0"`，但 `orchestrator/__init__.py` 没有导出 `__version__`，用户无法通过 `import orchestrator; print(orchestrator.__version__)` 获取版本号。

**修复方案**：在 `__init__.py` 中添加 `__version__ = "0.1.0"`，或使用 `importlib.metadata.version("data-orchestrator")` 动态读取。

### 1.3 包名与导入名不一致

**现状**：pip 安装包名是 `data-orchestrator`（带连字符），但导入名是 `orchestrator`。这在 `pyproject.toml` 中没有通过 `[tool.setuptools.packages]` 明确声明，且 `orchestrator` 这个名字过于通用，容易与其他包冲突。

**建议**：在 `pyproject.toml` 中添加 `[tool.setuptools.packages.find]` 配置明确指定包名；长期考虑是否将导入名改为 `data_orchestrator` 以与包名一致。

---

## 二、架构与设计缺陷（高优先级）

### 2.1 全局可变注册表存在线程安全和测试隔离问题

**现状**：`connectors/registry.py` 使用模块级 `_REGISTRY: dict` 作为全局状态，`notify/base.py` 同理用 `NOTIFIER_REGISTRY`。这带来两个问题：
1. **测试污染**：一个测试注册的 Connector 会泄漏到其他测试，`ConnectorAlreadyRegisteredError` 会在重复导入时触发
2. **无法并行运行多个 Orchestrator 实例**（各自用不同 Connector 集合）

**修复方案**：
- 提供 `reset_registry()` 函数供测试使用
- 长期可在 `Orchestrator` 实例内部持有自己的注册表，而非依赖全局变量
- 当前的 `@register_connector` 装饰器可保留为语法糖，但底层应转为实例级注册

### 2.2 `Orchestrator.__init__` 中创建 SQLAlchemy JobStore 的副作用

**现状**：`Orchestrator.__init__` 内部直接创建 `SQLAlchemyJobStore(url=self.settings.db_url)`，这意味着每次实例化都会连接（甚至创建）数据库。在测试、validate、list 等只读场景下不需要建立数据库连接。

**修复方案**：延迟初始化 `_scheduler`，仅在 `start()` 或 `register()` 时才创建 JobStore。或提供一个 `dry_run` 模式。

### 2.3 `_ORCHESTRATOR_INSTANCES` 全局字典导致内存泄漏

**现状**：`scheduler.py` 中 `_ORCHESTRATOR_INSTANCES` 是模块级字典，`Orchestrator.__init__` 将自身注册进去，`stop()` 时清理。但如果 `stop()` 未被调用（异常退出、忘记调用），实例永远不会被 GC 回收。

**修复方案**：
- 使用 `weakref.WeakValueDictionary` 替代普通 dict
- 或在 `__del__` 中也做清理（不推荐作为唯一手段）

### 2.4 `ping_all()` 用空 config 实例化 Connector

**现状**：`Orchestrator.ping_all()` 中对每个注册的 Connector 调用 `get_connector(connector_name, {})`，传入空字典作为 config。但大部分 Connector（如 Postgres）在 `initialize()` 时需要有效的 config（DSN 等），这会直接报错。

**修复方案**：`ping_all()` 应该基于已注册的 Pipeline 中实际使用的 connector_config 来实例化，而不是用空配置。或者改为只 ping 已实例化过的 Connector。

### 2.5 CLI `trigger`/`pause`/`resume` 命令重复加载

**现状**：CLI 中 `trigger`、`pause`、`resume` 命令内部先创建 `Orchestrator(config_dir=..., plugin_dir=...)`，然后又手动调用 `app.load_plugins()` 和 `app.load_config()`。而 `Orchestrator.__init__` 已经存储了 `config_dir` 和 `plugin_dir`，`ensure_loaded()` 会做同样的事情——这意味着插件和配置被加载了两次。

**修复方案**：统一使用 `ensure_loaded()` 模式，移除 CLI 中的手动重复加载。

---

## 三、可靠性与健壮性（高优先级）

### 3.1 模板变量在配置加载时就被渲染，而非执行时

**现状**：`ConfigLoader.load()` 调用 `load_yaml(file, variables=get_template_context())`，其中 `{{ yesterday }}` 等变量在**加载配置的那一刻**被渲染成具体日期。如果 Orchestrator 在凌晨 0 点启动，`{{ yesterday }}` 就固定为启动时的前一天，后续每天的调度执行都使用同一个日期值。

**这是一个严重 Bug**。

**修复方案**：配置加载时只解析 `${}` 环境变量，`{{ }}` Jinja 模板变量应推迟到 Task 实际执行时才渲染。可以在 `TaskRunner.run()` 中对 `action_kwargs` 做一次 Jinja 渲染。

### 3.2 `run_id` 在模板上下文中提前生成

**现状**：`get_template_context()` 每次调用都生成一个新的 `run_id`，但它是在配置加载阶段调用的，与实际的 Pipeline 执行 `run_id` 完全无关。用户在 YAML 中使用 `{{ run_id }}` 会得到一个毫无意义的、在加载时生成的 ID。

**修复方案**：与 3.1 一起修复，将 `run_id` 和 `pipeline_id` 在运行时注入。

### 3.3 Connector 实例未被复用，存在连接泄漏风险

**现状**：`TaskRunner.run()` 每次执行都调用 `get_connector(task.connector_name, task.connector_config)` 创建新实例，并在 `finally` 中 `close()`。对于 Postgres 等需要连接池的场景，每个 Task 执行都创建和销毁一个 SQLAlchemy Engine，效率极低。

**修复方案**：
- 在 Pipeline 级别维护 Connector 实例池（按 connector_name + config hash 去重）
- Pipeline 执行完毕后统一 close
- 可选：支持 Connector 级别的连接池配置

### 3.4 `ThreadPoolExecutor` 超时机制的隐患

**现状**：`TaskRunner` 中使用 `executor.submit(action, **kwargs)` + `future.result(timeout=...)` 实现超时。但 `future.result(timeout)` 超时后**不会终止正在运行的线程**——线程会继续执行直到完成。这意味着如果一个 Task 真的卡住了，线程池会逐渐耗尽。

**修复方案**：
- 在文档中明确说明超时的局限性（Python 线程不可强制终止）
- 考虑用 `multiprocessing` 或 `asyncio` + `cancel()` 替代
- 至少应在超时后记录一条 warning 日志

### 3.5 数据流传递中 `pass_output_from` 的容错不足

**现状**：当 `pass_output_from` 指定的上游 Task 失败或被跳过时，`upstream_results[task.pass_output_from].output` 会拿到 `None`，但没有任何显式检查或错误提示。下游 Task 会收到一个 `data=None`，可能产生难以排查的错误。

**修复方案**：在 `TaskRunner.run()` 中检查上游 Task 的状态，如果上游失败，则直接将当前 Task 标记为 SKIPPED 并给出明确原因。

---

## 四、功能完善（中优先级）

### 4.1 Settings 的全局默认值没有被应用

**现状**：`Settings` 定义了 `default_retry_times`、`default_timeout_seconds` 等字段，但 `TaskRunner` 和 `Task` 模型完全没有引用 `Settings`。Task 的默认值写死在 Pydantic model 里（`retry.times=3`、`timeout_seconds=60.0`），与 Settings 中的默认值无关。

**修复方案**：在 `PipelineRunner` 或 `Orchestrator` 层面，对未显式配置 retry/timeout 的 Task 应用 Settings 中的全局默认值。

### 4.2 缺少 async / 事件驱动支持

**现状**：所有执行都基于同步线程。对于 I/O 密集型的数据拉取场景（HTTP API、数据库查询），线程模型效率较低。

**建议**：
- 近期：不需要大改，但应为 BaseConnector 增加 `async_fetch` / `async_push` 可选方法
- 远期：支持 `asyncio` 调度器和异步 Connector

### 4.3 日志系统缺少结构化 Python logging

**现状**：框架几乎没有使用 Python 的 `logging` 模块。`LogWriter` 只往 SQLite 写入，`LogNotifier` 用 `rich.console.print()`。但标准 stdout/stderr 没有结构化的运行日志，出问题时难以定位。

**修复方案**：
- 在 `Orchestrator.start()`、`TaskRunner.run()` 等关键路径添加 `logging.getLogger(__name__)` 日志
- 支持 `Settings.log_level` 实际生效（当前只是定义了字段，没有调用 `logging.basicConfig()`）
- 考虑支持 JSON 格式日志输出

### 4.4 缺少 Pipeline 级别的 `on_success` / `on_failure` Hook

**现状**：Task 级别支持 `on_success` / `on_failure` hook，但 Pipeline 级别只有 notify。用户无法在 Pipeline 成功后自动触发另一个 Pipeline（链式调度）。

**建议**：增加 Pipeline 级 hook 或 trigger 机制，例如 `on_success: trigger another_pipeline`。

### 4.5 `schedule` 字段同时存在于 `Pipeline` model 和 `RegisteredPipeline`

**现状**：`Pipeline` 的 Pydantic model 有一个 `schedule: dict[str, Any]` 字段（原始字典），同时 `RegisteredPipeline` 有一个解析后的 `schedule: ScheduleConfig`。`Pipeline.schedule` 实际上被忽略了，只是白白占用空间。

**修复方案**：从 `Pipeline` model 中移除 `schedule` 字段，只在 `RegisteredPipeline` 层面保留。或让 `Pipeline` 直接持有 `ScheduleConfig`。

---

## 五、UI 与开发体验（中低优先级）

### 5.1 UI 与 Orchestrator 的耦合方式不稳定

**现状**：`streamlit_thread.py` 通过模块级全局变量 `_SHARED` 在主进程和 Streamlit 进程之间传递 `Orchestrator` 实例。但 Streamlit 实际上是通过 `bootstrap.run()` 启动的独立进程/线程，`get_orchestrator()` 在 Streamlit 进程中可能拿到 `None`（取决于进程模型）。

**修复方案**：
- 改用数据库作为唯一的状态共享层（UI 只读 DB + 写操作通过 API）
- 或引入一个轻量 HTTP API（FastAPI/Flask），UI 和 CLI 都通过 API 操作

### 5.2 缺少 `orchestrator init` 脚手架命令

**现状**：用户需要手动创建项目目录结构、`main.py`、`connectors/`、`pipelines/` 等。

**建议**：添加 `orchestrator init my_project` 命令，自动生成项目模板。

---


**需要注意的地方**：
- 异常体系复用了 Connector 的异常类给 Notifier 用（`ConnectorNotFoundError` / `ConnectorAlreadyRegisteredError`），语义不清，应为 Notifier 单独定义
- `TaskRunner._resolve_hook()` 通过 `importlib.import_module` 动态查找 hook 函数，但 hook 只能定义在 `orchestrator.notify` 模块中，过于受限
- `LogReader.get_dashboard_stats()` 中使用 `func.substr(started_at, 1, 13)` 做小时聚合，这依赖 ISO 格式字符串的字面值，换数据库可能出问题

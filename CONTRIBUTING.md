# Contributing Guide

## 如何添加新 Connector

1. 在 `orchestrator/connectors/` 或插件目录新建文件
2. 继承 `BaseConnector`，实现 `fetch / push / ping`
3. 使用 `@register_connector("name")` 完成注册
4. 在 `tests/connectors/test_{name}.py` 增加测试
5. 在 `examples/` 增加对应示例

## Connector 实现规范

- `ping()` 必须捕获所有异常并返回布尔值
- `push()` 必须考虑幂等行为
- 不要在 `__init__` 里做耗时网络调用
- 配置字段缺失时抛出清晰 `ValueError`，并说明缺失字段

## 测试规范

- Connector 测试文件路径统一为 `tests/connectors/test_{name}.py`
- 每个 Connector 必须覆盖以下场景：
  - fetch 成功
  - push 成功
  - ping 失败返回 False
  - 连接失败不抛异常

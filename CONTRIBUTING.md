# Contributing Guide

## 如何添加新 Connector

1. 在 `orchestrator/connectors/` 或插件目录新建文件
2. 继承 `BaseConnector`，只实现你需要的方法
3. 使用 `@register_connector("name")` 完成注册
4. 在 `tests/connectors/test_{name}.py` 增加测试
5. 在 `examples/` 增加对应示例

## Connector 实现规范

- 只写你需要的方法，不需要补空的 `fetch` / `push` / `ping`
- 如果实现了 `ping()`，必须捕获所有异常并返回布尔值
- 如果实现了 `push()`，必须考虑幂等行为
- 自定义 action 方法建议接受 `**kwargs`
- 不要在 `__init__` 里做耗时网络调用
- 配置字段缺失时抛出清晰 `ValueError`

## 测试规范

- Connector 测试文件路径统一为 `tests/connectors/test_{name}.py`
- 每个 Connector 只需覆盖它实际实现的方法
- 覆盖 action 方法的成功路径和错误处理路径
- 如果实现了 `ping`，测试成功和失败分支
- 未实现的方法不需要测试

# 03_parallel_tasks

这个示例演示 DAG 分层并发执行：

- `task_a/task_b/task_c` 无依赖，第一层并发执行
- `task_merge` 依赖前三个任务，第二层执行

运行方式：

```bash
orchestrator trigger parallel_demo --config examples/03_parallel_tasks/pipelines --plugins examples/03_parallel_tasks/connectors
```

# 02_shopify_to_postgres

这个示例演示真实业务链路：从 Shopify 拉取订单并写入 PostgreSQL。

## 准备环境变量

```bash
export SHOPIFY_BASE_URL="https://{shop}.myshopify.com/admin/api/2024-10"
export SHOPIFY_ACCESS_TOKEN="shpat_xxx"
export POSTGRES_DSN="postgresql://user:pass@localhost:5432/shop"
```

## 启动

```bash
orchestrator run --config examples/02_shopify_to_postgres/pipelines --plugins examples/02_shopify_to_postgres/connectors
```

## 手动触发

```bash
orchestrator trigger shopify_to_postgres_daily --config examples/02_shopify_to_postgres/pipelines --plugins examples/02_shopify_to_postgres/connectors
```

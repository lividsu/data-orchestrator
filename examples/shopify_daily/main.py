from orchestrator import Orchestrator

import examples.shopify_daily.connectors.shopify

app = Orchestrator(config_dir="examples/shopify_daily/pipelines")

if __name__ == "__main__":
    app.start()

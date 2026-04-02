from __future__ import annotations

from orchestrator import Orchestrator

app = Orchestrator(
    plugin_dir="examples/04_custom_action/connectors",
    config_dir="examples/04_custom_action/pipelines",
)

if __name__ == "__main__":
    app.start()

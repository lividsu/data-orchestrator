from __future__ import annotations

from orchestrator import Orchestrator

app = Orchestrator(
    config_dir="examples/01_minimal/pipelines",
    plugin_dir="examples/01_minimal/connectors",
)

if __name__ == "__main__":
    app.start()

from __future__ import annotations

from orchestrator import Orchestrator

app = Orchestrator(
    config_dir="examples/03_parallel_tasks/pipelines",
    plugin_dir="examples/03_parallel_tasks/connectors",
)

if __name__ == "__main__":
    app.start()

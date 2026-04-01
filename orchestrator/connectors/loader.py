from __future__ import annotations

import importlib.util
from pathlib import Path

from orchestrator.exceptions import ConfigNotFoundError


def load_plugins(plugin_dir: str | Path):
    plugin_path = Path(plugin_dir)
    if not plugin_path.exists():
        raise ConfigNotFoundError(f"Plugin directory not found: {plugin_path}")

    for py_file in plugin_path.glob("*.py"):
        if py_file.name.startswith("_"):
            continue
        module_name = py_file.stem
        spec = importlib.util.spec_from_file_location(module_name, py_file)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)


def load_builtin_connectors():
    import orchestrator.connectors.builtin

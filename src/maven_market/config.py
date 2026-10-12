"""Config-driven loader for the Maven Market lakehouse.

Everything that varies by environment or source lives in ``config/*.yml`` and
is read through here, so no paths, table names, or endpoints are hardcoded in
pipeline/ingestion code.

- ``load_config(env)`` deep-merges ``config/<env>.yml`` over ``config/defaults.yml``
  and resolves ``${VAR}`` / ``${VAR:default}`` references against the environment.
- ``get(cfg, "a.b.c")`` reads a nested value and raises ``KeyError`` if absent.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")

# Default config directory: <repo-root>/config  (this file is src/maven_market/config.py)
_DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _interpolate(value: Any) -> Any:
    """Recursively resolve ``${VAR}`` / ``${VAR:default}`` references in strings."""
    if isinstance(value, str):
        def _repl(match: re.Match) -> str:
            token = match.group(1)
            var, default = (token.split(":", 1) + [None])[:2] if ":" in token else (token, None)
            resolved = os.environ.get(var, default)
            if resolved is None:
                raise KeyError(f"Env var '{var}' is not set and no default was provided")
            return resolved
        return _ENV_PATTERN.sub(_repl, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def _deep_merge(base: dict, override: dict) -> dict:
    """Merge ``override`` onto ``base``; nested dicts merge, scalars/lists replace."""
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(env: str, config_dir: str | Path | None = None) -> dict:
    """Load merged, interpolated config for ``env`` (e.g. 'dev' or 'prod')."""
    config_dir = Path(config_dir) if config_dir else _DEFAULT_CONFIG_DIR
    env_path = config_dir / f"{env}.yml"
    if not env_path.exists():
        raise FileNotFoundError(f"No config file for env '{env}': {env_path}")

    defaults_path = config_dir / "defaults.yml"
    base = yaml.safe_load(defaults_path.read_text()) or {} if defaults_path.exists() else {}
    override = yaml.safe_load(env_path.read_text()) or {}

    return _interpolate(_deep_merge(base, override))


def get(cfg: dict, dotted_key: str) -> Any:
    """Read a nested value by dotted path; raise ``KeyError`` if any segment is missing."""
    node: Any = cfg
    for segment in dotted_key.split("."):
        if not isinstance(node, dict) or segment not in node:
            raise KeyError(f"Missing config key: '{dotted_key}' (at segment '{segment}')")
        node = node[segment]
    return node

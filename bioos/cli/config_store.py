import os
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_CONFIG_PATH = Path("~/.bioos/config.yaml").expanduser()
CONFIG_PATH_ENV = "BIOOS_CONFIG_PATH"
EXAMPLE_CONFIG = """client:
  MIRACLE_ACCESS_KEY: "AKxxxx"
  MIRACLE_SECRET_KEY: "SKxxxx"
  serveraddr: "https://bio-top.miracle.ac.cn"
  region: "cn-north-1"
"""


def get_config_path(explicit_path: Optional[str] = None) -> Path:
    if explicit_path:
        return Path(explicit_path).expanduser()
    env_path = os.getenv(CONFIG_PATH_ENV)
    if env_path:
        return Path(env_path).expanduser()
    return DEFAULT_CONFIG_PATH


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    config_path = get_config_path(path)
    if not config_path.is_file():
        return {}
    return _load_yaml_like(config_path.read_text(encoding="utf-8"))


def load_client_config(path: Optional[str] = None) -> Dict[str, Any]:
    config = load_config(path)
    client = config.get("client")
    if isinstance(client, dict):
        return client
    return {}


def _load_yaml_like(content: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    current_section: Optional[str] = None

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip())
        if stripped.endswith(":"):
            key = stripped[:-1].strip()
            if indent == 0:
                current_section = key
                data.setdefault(key, {})
            elif current_section:
                section = data.setdefault(current_section, {})
                if isinstance(section, dict):
                    section.setdefault(key, {})
            continue

        if ":" not in stripped:
            continue

        key, value = stripped.split(":", 1)
        parsed_value = _parse_scalar(value.strip())
        if indent > 0 and current_section:
            section = data.setdefault(current_section, {})
            if isinstance(section, dict):
                section[key.strip()] = parsed_value
        else:
            data[key.strip()] = parsed_value

    return data


def _parse_scalar(value: str) -> Any:
    if value in ('""', "''"):
        return ""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        return value[1:-1]
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return value

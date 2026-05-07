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
main_web:
  url: "https://cloud.miracle.ac.cn"
  login_token: "main site X-LoginToken"
  csrf_token: "main site csrfToken cookie value"
  cookie: "csrfToken=...; tenant=...; other web cookies"
repo:
  url: "https://network.miracle.ac.cn"
  token: "AAI token or session token"
  cookie: "ory_kratos_session=..."
datasite:
  url: "https://mclibrary.miracle.ac.cn"
  token: "AAI token or session token"
  cookie: "ory_kratos_session=..."
account:
  link_mode: "aai"
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


def save_config(config: Dict[str, Any], path: Optional[str] = None) -> Path:
    config_path = get_config_path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(_dump_yaml_like(config), encoding="utf-8")
    return config_path


def update_section_values(section: str, values: Dict[str, Any], path: Optional[str] = None) -> Path:
    config = load_config(path)
    existing = config.get(section)
    if not isinstance(existing, dict):
        existing = {}
    existing.update(values)
    config[section] = existing
    return save_config(config, path=path)


def load_client_config(path: Optional[str] = None) -> Dict[str, Any]:
    config = load_config(path)
    client = config.get("client")
    if isinstance(client, dict):
        return client
    return {}


def load_main_web_config(path: Optional[str] = None) -> Dict[str, Any]:
    config = load_config(path)
    main_web = config.get("main_web")
    if isinstance(main_web, dict):
        return main_web
    return {}


def load_repo_config(path: Optional[str] = None) -> Dict[str, Any]:
    config = load_config(path)
    repo = config.get("repo")
    if isinstance(repo, dict):
        return repo
    return {}


def load_datasite_config(path: Optional[str] = None) -> Dict[str, Any]:
    config = load_config(path)
    datasite = config.get("datasite")
    if isinstance(datasite, dict):
        return datasite
    return {}


def load_account_config(path: Optional[str] = None) -> Dict[str, Any]:
    config = load_config(path)
    account = config.get("account")
    if isinstance(account, dict):
        return account
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


def _dump_yaml_like(data: Dict[str, Any]) -> str:
    lines = []
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"{key}:")
            for sub_key, sub_value in value.items():
                lines.append(f"  {sub_key}: {_format_scalar(sub_value)}")
        else:
            lines.append(f"{key}: {_format_scalar(value)}")
    return "\n".join(lines) + "\n"


def _format_scalar(value: Any) -> str:
    if value is None:
        return '""'
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    escaped = text.replace('"', '\\"')
    return f'"{escaped}"'

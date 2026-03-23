from pathlib import Path
from typing import Dict

import requests


DEFAULT_BUILD_BASE_URL = "http://10.20.16.38:3001"


def get_docker_image_url(registry: str, namespace_name: str, repo_name: str, tag: str) -> str:
    return f"{registry}/{namespace_name}/{repo_name}:{tag}"


def build_docker_image_request(
    repo_name: str,
    tag: str,
    source_path: str,
    registry: str = "registry-vpc.miracle.ac.cn",
    namespace_name: str = "auto-build",
) -> Dict[str, object]:
    path = Path(source_path)
    if not path.exists():
        raise FileNotFoundError(f"Source path not found: {source_path}")
    with path.open("rb") as handle:
        response = requests.post(
            f"{DEFAULT_BUILD_BASE_URL}/build",
            files={"Source": handle},
            data={
                "Registry": registry,
                "NamespaceName": namespace_name,
                "RepoName": repo_name,
                "ToTag": tag,
            },
            timeout=120,
        )
    response.raise_for_status()
    result = response.json()
    result["ImageURL"] = get_docker_image_url(registry, namespace_name, repo_name, tag)
    return result


def check_build_status_request(task_id: str) -> Dict[str, object]:
    response = requests.get(f"{DEFAULT_BUILD_BASE_URL}/build/status/{task_id}", timeout=60)
    response.raise_for_status()
    return response.json()


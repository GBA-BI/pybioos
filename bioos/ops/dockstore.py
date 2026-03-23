import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests


DOCKSTORE_BASE_URL = "https://dockstore.miracle.ac.cn"
DOCKSTORE_API_BASE = f"{DOCKSTORE_BASE_URL}/api"
DOCKSTORE_SEARCH_URL = f"{DOCKSTORE_API_BASE}/api/ga4gh/v2/extended/tools/entry/_search"
DOCKSTORE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def _search_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "text/plain",
        "Origin": DOCKSTORE_BASE_URL,
        "Referer": f"{DOCKSTORE_BASE_URL}/search?descriptorType=WDL&entryType=workflows&searchMode=files",
        "User-Agent": DOCKSTORE_USER_AGENT,
    }


def _request_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": DOCKSTORE_USER_AGENT,
    }


def _build_search_body(query: Iterable[List[str]], sentence: bool, query_type: str) -> Dict[str, object]:
    body: Dict[str, object] = {
        "size": 201,
        "_source": [
            "author",
            "descriptorType",
            "full_workflow_path",
            "gitUrl",
            "name",
            "namespace",
            "organization",
            "providerUrl",
            "repository",
            "verified",
            "workflowName",
            "description",
            "categories",
            "all_authors",
            "input_file_formats",
            "output_file_formats",
        ],
        "query": {"bool": {"must": [{"match": {"_index": "workflows"}}, {"match_all": {}}]}},
    }
    should = []
    for field, _operator, term in query:
        if query_type == "wildcard":
            should.append({"wildcard": {field: {"value": f"*{term}*", "case_insensitive": True}}})
        else:
            match_type = "match_phrase" if sentence else "match"
            should.append({match_type: {field: {"query": term}}})
    if should:
        body["query"]["bool"]["should"] = should
        body["query"]["bool"]["minimum_should_match"] = 1
    return body


def _normalize_hit(hit: Dict[str, object], output_full: bool = False) -> Dict[str, object]:
    source = hit.get("_source", {})
    categories = [cat.get("name", "") for cat in source.get("categories", []) if isinstance(cat, dict)]
    authors = [author.get("name", "") for author in source.get("all_authors", []) if isinstance(author, dict)]
    record = {
        "name": source.get("workflowName") or source.get("name") or source.get("repository") or "Unnamed workflow",
        "path": source.get("full_workflow_path"),
        "url": f"{DOCKSTORE_BASE_URL}/workflows/{source.get('full_workflow_path')}",
        "description": (source.get("description") or "").split("\n")[0],
        "score": hit.get("_score"),
    }
    if output_full:
        record.update(
            {
                "descriptor_type": source.get("descriptorType"),
                "organization": source.get("organization"),
                "categories": categories,
                "authors": authors,
                "verified": source.get("verified"),
                "input_formats": [fmt.get("value", "") for fmt in source.get("input_file_formats", []) if isinstance(fmt, dict)],
                "output_formats": [fmt.get("value", "") for fmt in source.get("output_file_formats", []) if isinstance(fmt, dict)],
            }
        )
    return record


def search_dockstore_workflows(
    query: List[List[str]],
    top_n: int = 3,
    query_type: str = "match_phrase",
    sentence: bool = False,
    output_full: bool = False,
) -> Dict[str, object]:
    if not query:
        raise ValueError("At least one --query field/operator/term triple is required.")
    body = _build_search_body(query, sentence, query_type)
    response = requests.post(
        DOCKSTORE_SEARCH_URL,
        headers=_search_headers(),
        data=json.dumps(body),
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    hits = payload.get("hits", {}).get("hits", [])
    results = [_normalize_hit(hit, output_full=output_full) for hit in hits[:top_n]]
    return {"success": True, "query": query, "count": len(results), "results": results}


def parse_workflow_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    path = urlparse(url).path if url.startswith(("http://", "https://")) else url
    if "/workflows/" in path:
        path = path.split("/workflows/")[-1]
    parts = path.strip("/").split("/")
    if len(parts) < 3:
        return None, None
    if any(domain in parts[0] for domain in (".com", ".cn", ".org", ".io", ".net")):
        return parts[1], parts[-1]
    return parts[0], parts[-1]


def _get_published_workflows(organization: str) -> List[Dict[str, object]]:
    response = requests.get(
        f"{DOCKSTORE_API_BASE}/workflows/organization/{organization}/published",
        headers=_request_headers(),
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _find_workflow_by_name(workflows: List[Dict[str, object]], workflow_name: str) -> Optional[Dict[str, object]]:
    if not workflows:
        return None
    lowered = workflow_name.lower()
    for matcher in (
        lambda wf: wf.get("workflowName") == workflow_name,
        lambda wf: str(wf.get("workflowName", "")).lower() == lowered,
        lambda wf: str(wf.get("repository", "")).lower() == lowered,
        lambda wf: lowered in str(wf.get("workflowName", "")).lower() or lowered in str(wf.get("repository", "")).lower(),
    ):
        matches = [wf for wf in workflows if matcher(wf)]
        if matches:
            matches.sort(key=lambda item: item.get("lastUpdated", ""), reverse=True)
            return matches[0]
    return None


def _get_latest_workflow_version(workflow: Dict[str, object]) -> Optional[Dict[str, object]]:
    versions = workflow.get("workflowVersions", []) or []
    if not isinstance(versions, list) or not versions:
        return None
    stable = [version for version in versions if isinstance(version, dict) and version.get("valid", False)]
    target = stable or [version for version in versions if isinstance(version, dict)]
    target.sort(key=lambda item: item.get("lastUpdated", ""), reverse=True)
    return target[0] if target else None


def _get_source_files(workflow_id: int, version_id: int) -> List[Dict[str, object]]:
    response = requests.get(
        f"{DOCKSTORE_API_BASE}/workflows/{workflow_id}/workflowVersions/{version_id}/sourcefiles",
        headers=_request_headers(),
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _write_source_files(base_dir: Path, source_files: List[Dict[str, object]]) -> List[str]:
    saved_files = []
    for source_file in source_files:
        path = source_file.get("path")
        content = source_file.get("content")
        if not path or content is None:
            continue
        file_path = base_dir / path.lstrip("/")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
        saved_files.append(str(file_path.resolve()))
    return saved_files


def fetch_wdl_from_dockstore_url(url: str, output_path: str = ".") -> Dict[str, object]:
    organization, workflow_name = parse_workflow_url(url)
    if not organization or not workflow_name:
        raise ValueError(f"Unable to parse Dockstore workflow URL: {url}")

    workflows = _get_published_workflows(organization)
    workflow = _find_workflow_by_name(workflows, workflow_name)
    if not workflow:
        raise RuntimeError(f"Workflow not found for organization={organization}, workflow_name={workflow_name}")

    version = _get_latest_workflow_version(workflow)
    if not version:
        raise RuntimeError("Workflow has no downloadable version.")

    source_files = _get_source_files(workflow["id"], version["id"])
    save_dir = Path(output_path) / f"{organization}_{workflow_name}"
    save_dir.mkdir(parents=True, exist_ok=True)
    files = _write_source_files(save_dir, source_files)

    if not files:
        raise RuntimeError("No workflow source files were downloaded.")

    metadata = {
        "organization": organization,
        "workflow_name": workflow_name,
        "workflow_id": workflow.get("id"),
        "version_id": version.get("id"),
        "version_name": version.get("name"),
        "full_workflow_path": workflow.get("full_workflow_path"),
        "descriptor_type": workflow.get("descriptorType"),
    }
    metadata_path = save_dir / "workflow_metadata.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    wdl_dirs = []
    for root, dirs, filenames in os.walk(save_dir):
        if dirs:
            continue
        if any(name.endswith(".wdl") for name in filenames):
            wdl_dirs.append(str(Path(root).resolve()))

    return {
        "success": True,
        "organization": organization,
        "workflow_name": workflow_name,
        "save_directory": str(save_dir.resolve()),
        "files": files,
        "metadata_path": str(metadata_path.resolve()),
        "wdl_save_directory": wdl_dirs[0] if wdl_dirs else None,
    }

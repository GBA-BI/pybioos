# pybioos

Python SDK and CLI for Bio-OS.

[![Documentation Status](https://readthedocs.org/projects/pybioos/badge/?version=latest)](https://pybioos.readthedocs.io/en/latest/?badge=latest)

## What Changed

`pybioos` now provides a unified root CLI:

```bash
bioos --help
```

The CLI is organized by resource groups such as `workspace`, `workflow`, `submission`, `file`, `ies`, `dockstore`, and `docker`.

## Highlights

This release expands the unified `bioos` CLI in three main areas:

- resilient workspace file upload with checkpoint resume and retry support
- workspace member management commands
- account-level usage and resource query commands

It also makes workflow local-file preprocessing more robust and allows downloads from current-workspace `s3://...` paths.

## Installation

Install from PyPI:

```bash
pip install pybioos
```

Install from source:

```bash
git clone https://github.com/GBA-BI/pybioos.git
cd pybioos
pip install -e .
```

After installation, verify the CLI:

```bash
bioos --help
```

## Authentication

`pybioos` supports three authentication sources, in this order:

1. CLI flags: `--ak`, `--sk`, `--endpoint`
2. Environment variables: `MIRACLE_ACCESS_KEY`, `MIRACLE_SECRET_KEY`, `BIOOS_ENDPOINT`
3. Local config file: `~/.bioos/config.yaml`

Recommended approach:

- For local interactive use, prefer `~/.bioos/config.yaml`
- `--ak`, `--sk`, and `--endpoint` are currently kept for compatibility and automation scenarios
- These explicit authentication flags may be deprecated in a future release, so new integrations should avoid depending on them when possible

Recommended local config file:

```yaml
client:
  MIRACLE_ACCESS_KEY: "your-access-key"
  MIRACLE_SECRET_KEY: "your-secret-key"
  serveraddr: "https://bio-top.miracle.ac.cn"
  region: "cn-north-1"
```

You can inspect the resolved auth status with:

```bash
bioos auth status
```

You can also print the expected config path and an example payload:

```bash
bioos config path
bioos config example
```

## Legacy Compatibility

The following legacy commands are still available for compatibility:

- `bw`
- `bw_import`
- `bw_import_status_check`
- `bw_status_check`
- `get_submission_logs`

These commands are legacy compatibility entry points and may be removed in a future release.

New users should use the unified `bioos` command tree instead:

- `bw` -> `bioos workflow submit`
- `bw_import` -> `bioos workflow import`
- `bw_import_status_check` -> `bioos workflow import-status`
- `bw_status_check` -> `bioos workflow run-status`
- `get_submission_logs` -> `bioos submission logs`

## Quick Start

List workspaces:

```bash
bioos workspace list
```

Create a workspace:

```bash
bioos workspace create \
  --workspace-name my-workspace \
  --workspace-description "My Bio-OS workspace"
```

List workflows in a workspace:

```bash
bioos workflow list --workspace-name my-workspace
```

Generate an input template:

```bash
bioos workflow input-template \
  --workspace-name my-workspace \
  --workflow-name my-workflow
```

Import a workflow:

```bash
bioos workflow import \
  --workspace-name my-workspace \
  --workflow-name my-workflow \
  --workflow-source ./workflow.wdl
```

Submit a workflow:

```bash
bioos workflow submit \
  --workspace-name my-workspace \
  --workflow-name my-workflow \
  --input-json ./input.json
```

Check workflow run status:

```bash
bioos workflow run-status \
  --workspace-name my-workspace \
  --submission-id <submission-id>
```

Download submission logs:

```bash
bioos submission logs \
  --workspace-name my-workspace \
  --submission-id <submission-id>
```

List workspace files:

```bash
bioos file list --workspace-name my-workspace --recursive
```

Upload local files to a workspace:

```bash
bioos file upload \
  --workspace-name my-workspace \
  --source ./inputs/sample1.fastq.gz \
  --source ./inputs/sample2.fastq.gz \
  --target input_provision/ \
  --skip-existing
```

List workspace members:

```bash
bioos workspace member list \
  --workspace-name my-workspace \
  --page-size 100
```

Query account-level usage:

```bash
bioos usage resource-total \
  --start-time 1776214800 \
  --end-time 1776301200
```

## CLI Overview

Top-level command groups:

- `bioos auth`
- `bioos config`
- `bioos workspace`
- `bioos workflow`
- `bioos submission`
- `bioos file`
- `bioos ies`
- `bioos dockstore`
- `bioos docker`

Common examples:

```bash
bioos workspace --help
bioos workflow --help
bioos submission --help
```

Current workflow commands:

- `bioos workflow list`
- `bioos workflow input-template`
- `bioos workflow import`
- `bioos workflow import-status`
- `bioos workflow run-status`
- `bioos workflow submit`
- `bioos workflow validate`

Current workspace commands:

- `bioos workspace list`
- `bioos workspace create`
- `bioos workspace export`
- `bioos workspace profile`
- `bioos workspace dashboard-upload`
- `bioos workspace member list`
- `bioos workspace member add`
- `bioos workspace member update`
- `bioos workspace member delete`

Current file commands:

- `bioos file upload`
- `bioos file list`
- `bioos file download`
- `bioos file delete`

Current usage commands:

- `bioos usage asset-data`
- `bioos usage asset-list`
- `bioos usage asset-total`
- `bioos usage resource-data`
- `bioos usage resource-workspace-list`
- `bioos usage resource-user-list`
- `bioos usage resource-total`

## File Transfer

### Upload

`bioos file upload` uploads one or more local files into the current workspace bucket.

Key capabilities:

- multiple `--source` inputs in one command
- multipart upload for large files
- resumable uploads with checkpoint files
- per-file retry controls
- `--skip-existing` support
- shared upload behavior reused by workflow local-file preprocessing

Example:

```bash
bioos file upload \
  --workspace-name my-workspace \
  --source ./data/sample.bam \
  --target input_provision/ \
  --checkpoint-dir ~/.bioos/upload-checkpoints \
  --max-retries 3 \
  --task-num 10
```

Available upload options:

- `--workspace-name`: target workspace name
- `--source`: local file path, can be repeated
- `--target`: target prefix in the workspace bucket
- `--flatten/--no-flatten`: control whether local directory structure is preserved
- `--skip-existing`: skip upload when the target key already exists
- `--checkpoint-dir`: checkpoint directory for resumable multipart uploads
- `--max-retries`: retry count per file after the first attempt
- `--task-num`: multipart parallel task count

### Download

`bioos file download` supports:

- internal workspace object keys such as `input_provision/a.txt`
- current-workspace `s3://bucket/key` paths

If an `s3://...` path points to a different bucket than the current workspace bucket, the CLI raises a clear bucket mismatch error instead of attempting a cross-workspace download.

## Workflow Local File Preprocessing

`bioos workflow submit` automatically scans `input.json` for local file paths before submission.

Compared with the earlier implementation, the current behavior is more robust:

- recursively traverses real JSON structures instead of regex-scanning the serialized JSON string
- supports singleton `{...}` and batch `[{...}, {...}]` inputs
- correctly recognizes Chinese paths and paths containing spaces
- preserves non-local values such as `drs://...` and existing `s3://...`
- replaces only exact matched local-path values with uploaded `s3://...` locations

This preprocessing path reuses the same resilient upload helper as `bioos file upload`.

## Workspace Member Management

Workspace member operations are available under `bioos workspace member`.

Examples:

```bash
bioos workspace member list \
  --workspace-name my-workspace \
  --page-size 100

bioos workspace member add \
  --workspace-name my-workspace \
  --name alice \
  --name bob \
  --role User

bioos workspace member update \
  --workspace-name my-workspace \
  --name alice \
  --role Admin

bioos workspace member delete \
  --workspace-name my-workspace \
  --name alice
```

Notes:

- `member list` defaults to `InWorkspace=true`, so it lists users already in the workspace
- `member list` also supports `--page-number`, `--page-size`, `--keyword`, `--role`, and `--no-in-workspace`
- supported member roles are `Visitor`, `User`, and `Admin`

## Account-Level Usage Queries

Account-level usage commands are available under `bioos usage`.

Examples:

```bash
bioos usage asset-total \
  --start-time 1776214800 \
  --end-time 1776301200 \
  --type WorkspaceVisit

bioos usage resource-user-list \
  --start-time 1776214800 \
  --end-time 1776301200

bioos usage resource-data \
  --start-time 1776214800 \
  --end-time 1776301200 \
  --type cpu
```

Notes:

- usage APIs are account-level, not workspace-scoped
- some usage APIs are only available to the account owner
- when a sub-account calls owner-only usage APIs, pybioos returns a clearer permission error instead of exposing the raw backend error payload

## Compatibility Notes

- `tos` is still pinned to `2.5.6` in this release
- `pandas>=1.3.0` is still supported in this release
- workflow batch preprocessing currently uses a temporary `DataFrame.map/applymap` compatibility shim so the same code can run on both older pandas releases and newer environments such as pandas 3.x
- this pandas compatibility shim is transitional and is expected to be removed in the next release after the minimum supported pandas version is raised to `2.1+`
- legacy commands such as `bw` and `bw_import` are still available for compatibility, but new integrations should use the unified `bioos` command tree

## Current Limitations

- directory upload is not supported yet; `bioos file upload` currently accepts files, not whole directories
- `bioos file download` does not yet support direct signed `https://...` download URLs
- usage commands depend on backend permissions; owner-only APIs require the main account AK/SK

## Python SDK Example

```python
from bioos import bioos

bioos.login(
    endpoint="https://bio-top.miracle.ac.cn",
    access_key="your-access-key",
    secret_key="your-secret-key",
)

workspaces = bioos.list_workspaces()
print(workspaces)
```

Use a workspace object:

```python
from bioos import bioos

bioos.login(
    endpoint="https://bio-top.miracle.ac.cn",
    access_key="your-access-key",
    secret_key="your-secret-key",
)

workspace_id = "your-workspace-id"
ws = bioos.Workspace(workspace_id)
print(ws)
```

## Documentation

Full documentation is available at [https://pybioos.readthedocs.io/](https://pybioos.readthedocs.io/).

For notebook-style examples, see `Example_usage.ipynb`.

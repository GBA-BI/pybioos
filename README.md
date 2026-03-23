# pybioos

Python SDK and CLI for Bio-OS.

[![Documentation Status](https://readthedocs.org/projects/pybioos/badge/?version=latest)](https://pybioos.readthedocs.io/en/latest/?badge=latest)

## What Changed

`pybioos` now provides a unified root CLI:

```bash
bioos --help
```

The CLI is organized by resource groups such as `workspace`, `workflow`, `submission`, `file`, `ies`, `dockstore`, and `docker`.

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

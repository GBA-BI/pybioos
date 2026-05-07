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

## AAI, Repository, and Data Site

`pybioos` now supports the repository and data site APIs that are authenticated through the BioOS main web session and repository passport flow.

Service endpoints used in the current integration:

- BioOS main site: `https://cloud.miracle.ac.cn`
- Repository site: `https://network.miracle.ac.cn`
- Data site: `https://mclibrary.miracle.ac.cn`

There are two related authentication layers:

1. BioOS SDK authentication, used by workspace and workflow APIs
2. BioOS main-web session -> AAI passport exchange, used by repository and data-site APIs

In practice:

- `bioos auth ...` is for SDK-style AK/SK authentication
- `bioos aai ...` is for main-web login, AAI linkage check, passport exchange, and passport refresh
- `bioos repo ...` and `bioos datasite ...` use the saved AAI passport by default

Recommended workflow:

1. Log in to the BioOS main site in your browser.
2. Open browser developer tools and copy any main-site GraphQL request as cURL.
3. Run one command to extract the main-site auth materials, get a repository passport, and save it into local config:

```bash
bioos aai sync-from-curl --curl '...copied main-site cURL...' --pretty
```

If you also want to save the BioOS SDK credentials and then complete the AAI sync in one step, use:

```bash
bioos auth connect-aai \
  --ak 'your-access-key' \
  --sk 'your-secret-key' \
  --curl '...copied main-site cURL...' \
  --pretty
```

This command saves the `client` section, verifies BioOS SDK authentication, saves the resolved `main_web` materials, and then syncs the repository passport into both `repo` and `datasite`.

If you want the CLI to log in to the BioOS main site directly with account/password first, you can now use:

```bash
bioos aai login \
  --account-name 'your-account-name' \
  --password 'your-password' \
  --pretty
```

If the BioOS web account is not linked to an AAI repository account yet, the CLI now stops before the passport request and returns a clear linkage hint instead of failing later.

The saved `repo` and `datasite` sections now include `passport_issued_at` and `passport_expires_at`. You can refresh them with:

```bash
bioos aai refresh --pretty
```

Or refresh and re-login in one shot:

```bash
bioos aai refresh \
  --account-name 'your-account-name' \
  --password 'your-password' \
  --pretty
```

When a saved passport is expired, `bioos repo ...` and `bioos datasite ...` commands now fail fast with a message telling you to run `bioos aai refresh` or `bioos aai login`.

Or combine that with BioOS SDK credential persistence in one step:

```bash
bioos auth connect-aai \
  --ak 'your-access-key' \
  --sk 'your-secret-key' \
  --account-name 'your-account-name' \
  --password 'your-password' \
  --pretty
```

You can inspect the current AAI-related state at any time:

```bash
bioos aai status --pretty
bioos aai auth --service repo --pretty
bioos aai auth --service datasite --pretty
```

Typical local config structure after successful AAI login:

```yaml
main_web:
  url: "https://cloud.miracle.ac.cn"
  login_token: "main site X-LoginToken"
  csrf_token: "main site csrfToken"
  cookie: "tenant=...; csrfToken=...; ..."
repo:
  url: "https://network.miracle.ac.cn"
  token: "repository passport token"
  passport_issued_at: "2026-05-07T10:00:00+00:00"
  passport_expires_at: "2026-06-06T10:00:00+00:00"
datasite:
  url: "https://mclibrary.miracle.ac.cn"
  token: "repository passport token"
  passport_issued_at: "2026-05-07T10:00:00+00:00"
  passport_expires_at: "2026-06-06T10:00:00+00:00"
```

After that, repository and data site commands can use the saved token directly:

```bash
bioos repo dataset list --page 1 --size 15 --pretty
bioos datasite dataset list --page 1 --size 15 --pretty
```

For the data site dataset publishing workflow, the CLI now ships with ready-to-edit JSON examples under [examples/datasite](./examples/datasite) and can also print inline templates:

```bash
bioos datasite dataset template --kind create --pretty
bioos datasite dataset template --kind apply --pretty
bioos datasite dataset template --kind upsert-files --pretty
bioos datasite dataset template --kind release --pretty
bioos datasite dataset template --kind update-config --pretty
```

For data file registration, two template forms are now bundled:

- JSON request body for `bioos datasite dataset upsert-files`: [upsert-files.json](./examples/datasite/upsert-files.json)
- CSV description template used by the data site front-end import flow: [data-files-template.csv](./examples/datasite/data-files-template.csv)

### Data Site Data Set End-to-End Workflow

1. Check whether the target dataset name is already taken:

```bash
bioos datasite dataset check-create \
  --name "mc-dataset-demo" \
  --pretty
```

2. Create the dataset from the bundled example payload:

```bash
bioos datasite dataset create \
  --json-file ./examples/datasite/create-dataset.json \
  --pretty
```

Expected response shape:

```json
{
  "id": "replace-with-created-data-set-id"
}
```

3. Upload or register files under the new dataset:

```bash
bioos datasite dataset upsert-files \
  --data-set-id replace-with-created-data-set-id \
  --json-file ./examples/datasite/upsert-files.json \
  --pretty
```

The `upsert-files` JSON payload uses the real backend field names:

- `dataFiles`: file item list
- `duplicatedReplace`: whether to replace duplicate files
- each file item may include `name`, `description`, `createTime`, `updateTime`, `fileType`, `fileSize`, `accessURL`, `source`, and optional `checksums`
- in the current recommended workflow, users submit the original `s3://...` object location and the service later exposes the generated `drs://...` value in dataset/file query results

If you are preparing file metadata through the same spreadsheet-style process used by the web UI, start from [data-files-template.csv](./examples/datasite/data-files-template.csv). The current front-end validation expects exactly these columns:

```text
File name,File description,Date created,Last updated,File type,File size,Data source,Data path
```

CSV validation notes from the current front-end behavior:

- `Date created` and `Last updated` use `YY-MM-DD`
- `Data path` must start with `s3://` or `drs://`
- when `Data path` is `drs://...`, `File size` is required
- file names and data paths must be unique within the imported CSV

Operational recommendation:

- for normal submission into the data site, prefer placing the underlying object location in `accessURL` / `Data path` as `s3://...`
- treat `drsURL` as a server-generated access identifier returned by follow-up `dataset files`, `file list-drs`, or DRS object queries

4. Optionally update file-table column ordering for the front-end:

```bash
bioos datasite dataset update-config \
  --id replace-with-created-data-set-id \
  --json-file ./examples/datasite/update-config.json \
  --pretty
```

5. Inspect the dataset and file list:

```bash
bioos datasite dataset get \
  --id replace-with-created-data-set-id \
  --pretty

bioos datasite dataset files \
  --data-set-id replace-with-created-data-set-id \
  --page 1 \
  --size 20 \
  --pretty
```

6. Submit the publish/application step:

Before calling `apply`, update `dataSetID` inside [apply-dataset.json](./examples/datasite/apply-dataset.json) to the real dataset ID returned by `create`.

```bash
bioos datasite dataset apply \
  --json-file ./examples/datasite/apply-dataset.json \
  --pretty
```

7. Release the dataset when it is ready to be published:

```bash
bioos datasite dataset release \
  --id replace-with-created-data-set-id \
  --json-file ./examples/datasite/release-dataset.json \
  --pretty
```

8. If needed, revoke the published state later:

```bash
bioos datasite dataset revoke \
  --id replace-with-created-data-set-id \
  --json '{}'
```

9. If you need to remove registered files:

```bash
bioos datasite dataset delete-files \
  --data-set-id replace-with-created-data-set-id \
  --json-file ./examples/datasite/delete-files.json \
  --pretty
```

Notes for the workflow above:

- `create`, `apply`, `update`, `update-config`, `delete`, `release`, `revoke`, `upsert-files`, and `delete-files` all support both saved AAI auth and explicit `--token` / `--cookie` overrides
- the bundled example payloads are intentionally conservative and should be edited to match the real category, catalogue, links, contacts, and file metadata for your dataset
- the backend field names follow the real browser/API behavior, so keeping payload keys exactly as shown is recommended
- if your current runtime depends on cookie-auth in addition to the saved passport token, you can append `--cookie 'ory_kratos_session=...'` to the same commands

Useful inspection commands:

```bash
bioos aai status
bioos aai account status \
  --web-url https://cloud.miracle.ac.cn \
  --login-token '...' \
  --csrf-token '...' \
  --cookie '...' \
  --pretty
```

If you want to separate extraction and sync:

```bash
bioos aai import-main-web-curl --curl '...copied main-site cURL...' --pretty

bioos aai sync-from-bioos \
  --web-url https://cloud.miracle.ac.cn \
  --login-token '...' \
  --csrf-token '...' \
  --cookie '...' \
  --expires-in 2592000 \
  --pretty
```

## Data Access Workflow

The most common read-only data access path is:

1. log in through `bioos aai login`
2. list datasets from `datasite` or `repo`
3. inspect one dataset
4. list dataset files
5. extract `drs://...` values
6. resolve DRS into downloadable HTTPS access
7. download one file or batch-download a filtered subset

The CLI now supports this end to end.

### Data Access Commands

List data-site datasets:

```bash
bioos datasite dataset list --page 1 --size 15 --pretty
```

Get one dataset:

```bash
bioos datasite dataset get \
  --id <DATA_SET_ID> \
  --pretty
```

List files under one dataset:

```bash
bioos datasite dataset files \
  --data-set-id <DATA_SET_ID> \
  --page 1 \
  --size 100 \
  --pretty
```

List only DRS links from a dataset:

```bash
bioos datasite file list-drs \
  --data-set-id <DATA_SET_ID> \
  --page 1 \
  --size 100 \
  --pretty
```

Resolve a single DRS URL:

```bash
bioos datasite drs resolve \
  --drs-url 'drs://imc-drs.miracle.ac.cn/<OBJECT_ID>' \
  --pretty
```

Download a single DRS object:

```bash
bioos datasite drs download \
  --drs-url 'drs://imc-drs.miracle.ac.cn/<OBJECT_ID>' \
  --target ./downloads \
  --pretty
```

Batch-download filtered files from a dataset:

```bash
bioos datasite dataset download-files \
  --data-set-id <DATA_SET_ID> \
  --target ./downloads \
  --name-contains 'tumor' \
  --pretty
```

Useful filtering options for `bioos datasite dataset download-files`:

- `--name-contains`: case-insensitive substring filter on file name
- `--regex`: regular expression on file name
- `--drs-url`: exact-match one DRS URL
- `--limit`: cap the number of matched files
- `--dry-run`: preview matches without downloading

Examples:

```bash
bioos datasite dataset download-files \
  --data-set-id <DATA_SET_ID> \
  --target ./downloads \
  --regex '.*\\.(fastq|fastq\\.gz|bam)$' \
  --limit 10 \
  --pretty

bioos datasite dataset download-files \
  --data-set-id <DATA_SET_ID> \
  --target ./downloads \
  --name-contains 'tumor' \
  --dry-run \
  --pretty
```

## Complete Example: Login to Batch Download

This example shows a complete data-access chain from BioOS main-site login to dataset file download.

### Step 1: Log in through BioOS main-web and sync AAI passport

```bash
bioos aai login \
  --account-name 'your-account-name' \
  --password 'your-password' \
  --pretty
```

Expected outcome:

- main-web session materials are saved into `main_web`
- AAI linkage is checked before passport exchange
- if linked, repository passport is saved into both `repo` and `datasite`

### Step 2: Verify passport status

```bash
bioos aai status --pretty
```

Look for:

- `repo.token_configured: true`
- `datasite.token_configured: true`
- `repo.passport_status: valid`
- `datasite.passport_status: valid`

If the passport is expired:

```bash
bioos aai refresh --pretty
```

### Step 3: Find a dataset

```bash
bioos datasite dataset list \
  --page 1 \
  --size 15 \
  --pretty
```

Pick one `id` from the output. In the examples below, suppose the dataset ID is:

```text
td45e334illb3o00f6o9g
```

### Step 4: Inspect the dataset and its files

```bash
bioos datasite dataset get \
  --id td45e334illb3o00f6o9g \
  --pretty

bioos datasite dataset files \
  --data-set-id td45e334illb3o00f6o9g \
  --page 1 \
  --size 100 \
  --pretty
```

### Step 5: Extract DRS URLs

```bash
bioos datasite file list-drs \
  --data-set-id td45e334illb3o00f6o9g \
  --page 1 \
  --size 100 \
  --pretty
```

Suppose one returned item contains:

```text
drs://imc-drs.miracle.ac.cn/ect4n4cphqtu9gcn9t05g
```

### Step 6: Resolve and download one file

```bash
bioos datasite drs resolve \
  --drs-url 'drs://imc-drs.miracle.ac.cn/ect4n4cphqtu9gcn9t05g' \
  --pretty

bioos datasite drs download \
  --drs-url 'drs://imc-drs.miracle.ac.cn/ect4n4cphqtu9gcn9t05g' \
  --target ./downloads \
  --pretty
```

### Step 7: Batch-download matching files from the same dataset

Preview matches first:

```bash
bioos datasite dataset download-files \
  --data-set-id td45e334illb3o00f6o9g \
  --target ./downloads \
  --name-contains 'fastq' \
  --dry-run \
  --pretty
```

Then perform the real download:

```bash
bioos datasite dataset download-files \
  --data-set-id td45e334illb3o00f6o9g \
  --target ./downloads \
  --name-contains 'fastq' \
  --pretty
```

Or use a stricter regex:

```bash
bioos datasite dataset download-files \
  --data-set-id td45e334illb3o00f6o9g \
  --target ./downloads \
  --regex '.*\\.(fastq|fastq\\.gz)$' \
  --limit 20 \
  --pretty
```

### Notes for the End-to-End Data Chain

- `dataset files` returns file metadata; `drsURL` is usually the key bridge to actual download
- `drs resolve` helps when you need to inspect object and access details before downloading
- `drs download` is best for one known file
- `dataset download-files` is best for repeated operational use on one dataset
- if you accidentally paste a malformed DRS URL, such as one with an extra trailing quote, the object lookup will fail with 404
- if a saved passport expires, `repo` and `datasite` commands now fail with a refresh hint rather than a vague downstream auth error

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

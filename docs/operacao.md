---
title: Operations
kind: runbook
area: operations
project: mcp-airflow
collection: mcp-airflow
owner: maintainers
status: current
canonical: docs/operacao.md
globalRef: qmd://mcp-airflow/docs/operacao.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs:
  - README.md
  - pyproject.toml
  - .github/workflows/ci.yml
  - .github/workflows/publish.yml
related:
  - docs/arquitetura.md
  - docs/fluxos-negocio.md
supersedes: []
supersededBy: []
sensitivity: public
---
# Operations

## Runtime configuration

| Variable | Required | Purpose |
|---|---|---|
| `AIRFLOW_BASE_URL` | yes | REST API base URL, including `/api/v1` or `/api/v2` |
| `AIRFLOW_USERNAME` | yes | Airflow authentication username |
| `AIRFLOW_PASSWORD` | yes | Airflow authentication password |

Provide credentials through the MCP host configuration or an authorized secret
adapter. Prefer HTTPS or a trusted private network; Basic Auth over plaintext
HTTP exposes credentials to the network path.

## Local validation

```bash
uv sync --group dev
uv run --all-extras --all-groups pytest -q
uvx ruff check src/ tests/
uvx ruff format --check src/ tests/
```

An integration smoke test should use a non-production Airflow account with the
minimum permissions needed by the selected tools. Do not print environment
variables, bearer tokens, or authenticated request headers while diagnosing.

## Release process

`pyproject.toml` is the package-version source of truth. GitHub releases trigger
trusted publishing to PyPI. The current workflow builds the distribution during
the release event; GitHub issue #11 tracks moving package build, metadata, and
isolated-install validation into pull-request CI.

Before publishing:

- ensure CI passes on supported Python versions;
- confirm the version and release tag match;
- verify the wheel can import `mcp_airflow.server` and expose the CLI entry
  point;
- review dependency bounds and public documentation;
- publish once through trusted publishing and verify the PyPI artifact.

## Failure interpretation

- A tool HTTP error means Airflow rejected or failed the request; it does not
  authorize retrying a mutation blindly.
- A successful `trigger_dag_run` means Airflow accepted the run, not that tasks
  completed.
- A healthy scheduler response does not prove DAG correctness or downstream
  data quality.
- Authentication fallback behavior is provisional until issue #10 is resolved.

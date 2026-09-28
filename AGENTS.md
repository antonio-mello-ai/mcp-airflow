---
title: AGENTS.md — MCP Airflow
kind: policy
area: engineering
project: mcp-airflow
collection: mcp-airflow
owner: maintainers
status: current
canonical: AGENTS.md
globalRef: qmd://mcp-airflow/AGENTS.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs: []
related:
  - README.md
  - docs/fluxos-negocio.md
  - docs/arquitetura.md
  - docs/operacao.md
  - docs/index.md
supersedes:
  - CLAUDE.md
  - GEMINI.md
supersededBy: []
sensitivity: public
---
# AGENTS.md — MCP Airflow

## Purpose

Public MCP server that exposes a bounded set of Apache Airflow REST API
operations as tools. The repository is an open-source package, not the source
of truth for any private Airflow deployment or credentials.

## Repository rules

- Keep examples generic and safe for a public repository.
- Never commit Airflow credentials, tokens, private hostnames, customer names,
  or deployment-specific topology.
- Preserve compatibility with the supported Airflow 2.x and 3.x API contracts.
- Treat `trigger_dag_run` and any future mutating tool as an operational action;
  its name, arguments, and result must make the mutation explicit.
- Do not silently broaden the MCP server's permissions or tool surface.
- Roadmap, backlog, and priority live in GitHub Issues. Delivery evidence lives
  in closed Issues, pull requests, releases, and PyPI publication records.
- Do not create `roadmap.md`, `docs/backlog.md`, or `CHANGELOG.md`.

## Development

```bash
uv sync --group dev
uv run --all-extras --all-groups pytest
uvx ruff check src/ tests/
uvx ruff format --check src/ tests/
```

`pyproject.toml` is the single source of truth for the package version. The
legacy `VERSION` file no longer exists. Until GitHub issue #9 is implemented,
do not enable the stale `.githooks/pre-push` hook described in older clones.

## Verification

- Add tests for every behavior change.
- Validate both the relevant Airflow API version and failure path.
- Never log or assert plaintext passwords, bearer tokens, or full authenticated
  URLs.
- Keep the package import and registered-tool smoke tests passing.

## Active documentation

- `README.md`: public overview and installation
- `docs/fluxos-negocio.md`: supported operator flows
- `docs/arquitetura.md`: components and API boundaries
- `docs/operacao.md`: configuration, validation, and releases
- `docs/index.md`: documentation index

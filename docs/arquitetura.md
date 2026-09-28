---
title: Architecture
kind: architecture
area: engineering
project: mcp-airflow
collection: mcp-airflow
owner: maintainers
status: current
canonical: docs/arquitetura.md
globalRef: qmd://mcp-airflow/docs/arquitetura.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs:
  - pyproject.toml
  - src/mcp_airflow/server.py
  - src/mcp_airflow/client.py
  - src/mcp_airflow/config.py
related:
  - docs/fluxos-negocio.md
  - docs/operacao.md
supersedes: []
supersededBy: []
sensitivity: public
---
# Architecture

## Components

- `server.py` creates the FastMCP server and registers the tool modules.
- `config.py` loads the Airflow base URL and credentials from environment
  variables.
- `client.py` owns the shared asynchronous HTTP client and authentication.
- `tools/dags.py` exposes DAG discovery and run-status reads.
- `tools/health.py` exposes failed-run and scheduler-health reads.
- `tools/runs.py` exposes task-instance inspection and manual DAG triggering.

The package uses the official MCP Python SDK and `httpx`. It does not persist
Airflow data or credentials.

## Request path

1. The MCP client starts `mcp-airflow` over the configured transport.
2. FastMCP validates the tool arguments and calls the registered function.
3. The shared Airflow client authenticates and sends a REST API request.
4. The tool converts the Airflow JSON response into a concise text result.

## Authentication boundary

For Airflow 3.x, the client obtains a bearer token from `/auth/token` and uses
the configured `/api/v2` base URL. For Airflow 2.x, it can use Basic Auth with
an `/api/v1` base URL.

The current automatic fallback catches every JWT failure before trying Basic
Auth. GitHub issue #10 tracks the required distinction between a valid Airflow
2.x fallback and authentication, TLS, network, or response failures.

Credentials stay in the process environment and must be provided by the MCP
host or an authorized secret adapter. They must never be written into MCP tool
results, logs, source files, or example configuration.

## Compatibility boundary

Airflow 2.x and 3.x differ in authentication, API prefix, filters, and response
fields. Compatibility logic must be explicit and tested; a successful import or
health response alone does not validate all tool contracts.

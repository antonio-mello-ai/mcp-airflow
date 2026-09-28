---
title: Supported operator flows
kind: source_doc
area: product
project: mcp-airflow
collection: mcp-airflow
owner: maintainers
status: current
canonical: docs/fluxos-negocio.md
globalRef: qmd://mcp-airflow/docs/fluxos-negocio.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs:
  - src/mcp_airflow/tools/dags.py
  - src/mcp_airflow/tools/health.py
  - src/mcp_airflow/tools/runs.py
related:
  - README.md
  - docs/arquitetura.md
  - docs/operacao.md
supersedes: []
supersededBy: []
sensitivity: public
---
# Supported operator flows

## Discover DAGs

`list_dags` returns the DAGs visible to the configured Airflow account and marks
each one as active or paused. Collection pagination is not yet transparent;
GitHub issue #2 tracks that limitation.

## Monitor recent execution

- `get_dag_runs_today` lists runs whose `run_after` is on the current UTC date.
- `check_failed_dags` lists failed runs from the last 24 hours.
- `get_dag_run_status` returns one run for a specific DAG. GitHub issue #8
  tracks the missing deterministic latest-run ordering.
- `check_scheduler_health` reads the Airflow root health endpoint and reports
  scheduler and metadatabase status.

These tools report API state; they do not prove business completion, downstream
data quality, or the health of systems outside Airflow.

## Inspect a run

`get_task_instances` lists task instances for a DAG run with state, duration,
and operator. Fetching task logs is not implemented and remains tracked in
GitHub issue #3.

## Trigger a run

`trigger_dag_run` creates a manual DAG run immediately. It is the only mutating
tool in the current package. The caller should name the intended DAG explicitly
and treat a successful API response as acceptance by Airflow, not proof that the
workflow completed successfully.

Pause/unpause and task-instance retry operations remain future work in GitHub
issue #4. They must preserve an explicit operational boundary before being
added.

## Additional read models

Read-only access to Airflow Variables is proposed in GitHub issue #6. Roadmap
items stay in GitHub rather than this document.

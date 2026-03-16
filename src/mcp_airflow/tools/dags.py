"""DAG-related tools."""

from __future__ import annotations

from datetime import UTC, datetime

from mcp_airflow.client import airflow_get
from mcp_airflow.server import mcp


@mcp.tool()
async def list_dags() -> str:
    """List all DAGs with their paused/active status.

    Returns a summary of every DAG registered in Airflow, including
    the dag_id, whether it is paused, and whether it is active.
    """
    data = await airflow_get("/dags")
    dags = data.get("dags", [])

    if not dags:
        return "No DAGs found."

    lines: list[str] = [f"Found {len(dags)} DAG(s):\n"]
    for dag in dags:
        status = "PAUSED" if dag.get("is_paused") else "ACTIVE"
        lines.append(f"  - {dag['dag_id']} [{status}]")

    return "\n".join(lines)


@mcp.tool()
async def get_dag_runs_today() -> str:
    """Get all DAG runs from today with their status.

    Returns every DAG run that started on the current UTC date,
    grouped by status (success, failed, running, queued).
    """
    today = datetime.now(UTC).strftime("%Y-%m-%dT00:00:00Z")
    data = await airflow_get(
        "/dags/~/dagRuns",
        params={
            "execution_date_gte": today,
            "order_by": "-execution_date",
        },
    )
    runs = data.get("dag_runs", [])

    if not runs:
        return "No DAG runs found for today."

    lines: list[str] = [f"Today's DAG runs ({len(runs)}):\n"]
    for run in runs:
        dag_id = run.get("dag_id", "unknown")
        state = run.get("state", "unknown")
        execution_date = run.get("execution_date", "")
        lines.append(f"  - {dag_id}: {state} (started {execution_date})")

    return "\n".join(lines)


@mcp.tool()
async def get_dag_run_status(dag_id: str) -> str:
    """Get the latest run status for a specific DAG.

    Args:
        dag_id: The DAG identifier.

    Returns the state, execution date, and duration of the most recent run.
    """
    data = await airflow_get(
        f"/dags/{dag_id}/dagRuns",
        params={"order_by": "-execution_date", "limit": 1},
    )
    runs = data.get("dag_runs", [])

    if not runs:
        return f"No runs found for DAG '{dag_id}'."

    run = runs[0]
    state = run.get("state", "unknown")
    execution_date = run.get("execution_date", "")
    run_id = run.get("dag_run_id", "")

    return (
        f"DAG: {dag_id}\n  Run ID: {run_id}\n  State: {state}\n  Execution date: {execution_date}"
    )

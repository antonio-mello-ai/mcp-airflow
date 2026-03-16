"""DAG run and task instance tools."""

from __future__ import annotations

from mcp_airflow.client import airflow_get, airflow_post
from mcp_airflow.server import mcp


@mcp.tool()
async def trigger_dag_run(dag_id: str) -> str:
    """Trigger a manual DAG run.

    Args:
        dag_id: The DAG identifier to trigger.

    Returns confirmation with the new run ID and state.
    """
    data = await airflow_post(f"/dags/{dag_id}/dagRuns", json_body={})

    run_id = data.get("dag_run_id", "unknown")
    state = data.get("state", "unknown")
    execution_date = data.get("execution_date", "")

    return (
        f"DAG '{dag_id}' triggered successfully.\n"
        f"  Run ID: {run_id}\n"
        f"  State: {state}\n"
        f"  Execution date: {execution_date}"
    )


@mcp.tool()
async def get_task_instances(dag_id: str, run_id: str) -> str:
    """Get individual task instances for a specific DAG run.

    Args:
        dag_id: The DAG identifier.
        run_id: The DAG run identifier.

    Returns the list of tasks with their state, duration, and operator.
    """
    data = await airflow_get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    tasks = data.get("task_instances", [])

    if not tasks:
        return f"No task instances found for DAG '{dag_id}', run '{run_id}'."

    lines: list[str] = [f"Tasks for {dag_id} / {run_id} ({len(tasks)}):\n"]
    for task in tasks:
        task_id = task.get("task_id", "unknown")
        state = task.get("state", "unknown")
        duration = task.get("duration")
        operator = task.get("operator", "")
        duration_str = f" ({duration:.1f}s)" if duration is not None else ""
        lines.append(f"  - {task_id}: {state}{duration_str} [{operator}]")

    return "\n".join(lines)

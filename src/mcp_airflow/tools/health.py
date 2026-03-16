"""Health and monitoring tools."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mcp_airflow.client import airflow_get
from mcp_airflow.server import mcp


@mcp.tool()
async def check_failed_dags() -> str:
    """Check for failed DAG runs in the last 24 hours.

    Returns a list of DAGs that had at least one failed run
    in the past 24 hours, with execution dates.
    """
    since = (datetime.now(UTC) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    data = await airflow_get(
        "/dags/~/dagRuns",
        params={
            "state": "failed",
            "execution_date_gte": since,
            "order_by": "-execution_date",
        },
    )
    runs = data.get("dag_runs", [])

    if not runs:
        return "No failed DAG runs in the last 24 hours."

    lines: list[str] = [f"Failed DAG runs in last 24h ({len(runs)}):\n"]
    for run in runs:
        dag_id = run.get("dag_id", "unknown")
        execution_date = run.get("execution_date", "")
        lines.append(f"  - {dag_id} (failed at {execution_date})")

    return "\n".join(lines)


@mcp.tool()
async def check_scheduler_health() -> str:
    """Check the Airflow scheduler health via the /health endpoint.

    Returns the scheduler status and latest heartbeat timestamp.
    """
    data = await airflow_get("/health")

    scheduler = data.get("scheduler", {})
    status = scheduler.get("status", "unknown")
    heartbeat = scheduler.get("latest_scheduler_heartbeat", "N/A")

    metadatabase = data.get("metadatabase", {})
    db_status = metadatabase.get("status", "unknown")

    return (
        f"Airflow Health:\n"
        f"  Scheduler: {status}\n"
        f"  Latest heartbeat: {heartbeat}\n"
        f"  Metadatabase: {db_status}"
    )

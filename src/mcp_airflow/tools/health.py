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
        },
    )
    runs = data.get("dag_runs", [])

    if not runs:
        return "No failed DAG runs in the last 24 hours."

    lines: list[str] = [f"Failed DAG runs in last 24h ({len(runs)}):\n"]
    for run in runs:
        dag_id = run.get("dag_id", "unknown")
        logical_date = run.get("logical_date", run.get("execution_date", ""))
        lines.append(f"  - {dag_id} (failed at {logical_date})")

    return "\n".join(lines)


@mcp.tool()
async def check_scheduler_health() -> str:
    """Check the Airflow scheduler health via the /health endpoint.

    Returns the scheduler status and latest heartbeat timestamp.
    """
    # /health is at the root, not under /api/v2
    import httpx

    from mcp_airflow.client import get_config

    config = get_config()
    from urllib.parse import urlparse

    parsed = urlparse(config.base_url)
    root_url = f"{parsed.scheme}://{parsed.netloc}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(f"{root_url}/health")
        response.raise_for_status()
        data = response.json()

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

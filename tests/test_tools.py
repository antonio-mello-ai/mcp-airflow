"""Tests for MCP Airflow tools."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

# Mock the config before importing tools so the client module doesn't fail
with patch.dict(
    "os.environ",
    {
        "AIRFLOW_BASE_URL": "http://localhost:8080/api/v1",
        "AIRFLOW_USERNAME": "admin",
        "AIRFLOW_PASSWORD": "admin",
    },
):
    from mcp_airflow.tools.dags import get_dag_run_status, get_dag_runs_today, list_dags
    from mcp_airflow.tools.health import check_failed_dags, check_scheduler_health
    from mcp_airflow.tools.runs import get_task_instances, trigger_dag_run


@pytest.fixture(autouse=True)
def _reset_client():
    """Reset the global client between tests."""
    import mcp_airflow.client as client_mod

    client_mod._client = None
    client_mod._config = None


# --- list_dags ---


@pytest.mark.asyncio
async def test_list_dags_returns_dags():
    mock_response = {
        "dags": [
            {"dag_id": "etl_daily", "is_paused": False},
            {"dag_id": "backup_weekly", "is_paused": True},
        ]
    }
    with patch("mcp_airflow.tools.dags.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        result = await list_dags()

    assert "etl_daily" in result
    assert "ACTIVE" in result
    assert "backup_weekly" in result
    assert "PAUSED" in result
    mock_get.assert_called_once_with("/dags")


@pytest.mark.asyncio
async def test_list_dags_empty():
    with patch("mcp_airflow.tools.dags.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {"dags": []}
        result = await list_dags()

    assert "No DAGs found" in result


# --- get_dag_runs_today ---


@pytest.mark.asyncio
async def test_get_dag_runs_today():
    mock_response = {
        "dag_runs": [
            {
                "dag_id": "etl_daily",
                "state": "success",
                "execution_date": "2026-03-16T06:00:00Z",
            },
        ]
    }
    with patch("mcp_airflow.tools.dags.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        result = await get_dag_runs_today()

    assert "etl_daily" in result
    assert "success" in result


@pytest.mark.asyncio
async def test_get_dag_runs_today_empty():
    with patch("mcp_airflow.tools.dags.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {"dag_runs": []}
        result = await get_dag_runs_today()

    assert "No DAG runs found for today" in result


# --- get_dag_run_status ---


@pytest.mark.asyncio
async def test_get_dag_run_status():
    mock_response = {
        "dag_runs": [
            {
                "dag_run_id": "manual__2026-03-16",
                "state": "running",
                "execution_date": "2026-03-16T10:00:00Z",
            },
        ]
    }
    with patch("mcp_airflow.tools.dags.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        result = await get_dag_run_status("etl_daily")

    assert "etl_daily" in result
    assert "running" in result
    assert "manual__2026-03-16" in result


@pytest.mark.asyncio
async def test_get_dag_run_status_no_runs():
    with patch("mcp_airflow.tools.dags.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {"dag_runs": []}
        result = await get_dag_run_status("missing_dag")

    assert "No runs found" in result


# --- trigger_dag_run ---


@pytest.mark.asyncio
async def test_trigger_dag_run():
    mock_response = {
        "dag_run_id": "manual__2026-03-16T12:00:00",
        "state": "queued",
        "execution_date": "2026-03-16T12:00:00Z",
    }
    with patch("mcp_airflow.tools.runs.airflow_post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response
        result = await trigger_dag_run("etl_daily")

    assert "triggered successfully" in result
    assert "queued" in result
    mock_post.assert_called_once_with("/dags/etl_daily/dagRuns", json_body={})


# --- get_task_instances ---


@pytest.mark.asyncio
async def test_get_task_instances():
    mock_response = {
        "task_instances": [
            {
                "task_id": "extract",
                "state": "success",
                "duration": 12.5,
                "operator": "PythonOperator",
            },
            {
                "task_id": "transform",
                "state": "failed",
                "duration": 45.2,
                "operator": "PythonOperator",
            },
        ]
    }
    with patch("mcp_airflow.tools.runs.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        result = await get_task_instances("etl_daily", "run_123")

    assert "extract" in result
    assert "success" in result
    assert "12.5s" in result
    assert "transform" in result
    assert "failed" in result


@pytest.mark.asyncio
async def test_get_task_instances_empty():
    with patch("mcp_airflow.tools.runs.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {"task_instances": []}
        result = await get_task_instances("etl_daily", "run_123")

    assert "No task instances found" in result


# --- check_failed_dags ---


@pytest.mark.asyncio
async def test_check_failed_dags():
    mock_response = {
        "dag_runs": [
            {
                "dag_id": "etl_broken",
                "execution_date": "2026-03-16T03:00:00Z",
            },
        ]
    }
    with patch("mcp_airflow.tools.health.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        result = await check_failed_dags()

    assert "etl_broken" in result
    assert "failed" in result.lower()


@pytest.mark.asyncio
async def test_check_failed_dags_none():
    with patch("mcp_airflow.tools.health.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {"dag_runs": []}
        result = await check_failed_dags()

    assert "No failed DAG runs" in result


# --- check_scheduler_health ---


@pytest.mark.asyncio
async def test_check_scheduler_health():
    mock_response = {
        "scheduler": {
            "status": "healthy",
            "latest_scheduler_heartbeat": "2026-03-16T12:00:00Z",
        },
        "metadatabase": {"status": "healthy"},
    }
    with patch("mcp_airflow.tools.health.airflow_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        result = await check_scheduler_health()

    assert "healthy" in result
    assert "Scheduler" in result
    assert "Metadatabase" in result
    mock_get.assert_called_once_with("/health")

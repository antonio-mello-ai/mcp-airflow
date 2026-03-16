"""HTTP client wrapper for Airflow REST API."""

from __future__ import annotations

import httpx

from mcp_airflow.config import AirflowConfig

_client: httpx.AsyncClient | None = None
_config: AirflowConfig | None = None


def get_config() -> AirflowConfig:
    """Return cached config, loading from env on first call."""
    global _config
    if _config is None:
        _config = AirflowConfig.from_env()
    return _config


async def get_client() -> httpx.AsyncClient:
    """Return a shared httpx.AsyncClient with basic auth configured.

    The client is created lazily and reused across requests.
    """
    global _client
    if _client is None or _client.is_closed:
        config = get_config()
        _client = httpx.AsyncClient(
            base_url=config.base_url,
            auth=httpx.BasicAuth(config.username, config.password),
            timeout=httpx.Timeout(30.0),
            headers={"Content-Type": "application/json"},
        )
    return _client


async def airflow_get(path: str, params: dict | None = None) -> dict:
    """Execute a GET request against the Airflow API.

    Args:
        path: API path relative to base URL (e.g., "/dags").
        params: Optional query parameters.

    Returns:
        Parsed JSON response as a dict.

    Raises:
        httpx.HTTPStatusError: If the response status is 4xx/5xx.
    """
    client = await get_client()
    response = await client.get(path, params=params)
    response.raise_for_status()
    return response.json()


async def airflow_post(path: str, json_body: dict | None = None) -> dict:
    """Execute a POST request against the Airflow API.

    Args:
        path: API path relative to base URL.
        json_body: Optional JSON body.

    Returns:
        Parsed JSON response as a dict.

    Raises:
        httpx.HTTPStatusError: If the response status is 4xx/5xx.
    """
    client = await get_client()
    response = await client.post(path, json=json_body or {})
    response.raise_for_status()
    return response.json()

"""HTTP client wrapper for Airflow REST API.

Supports both Airflow 2.x (basic auth) and Airflow 3.x (JWT auth).
JWT tokens are obtained automatically and refreshed on 401.
"""

from __future__ import annotations

import logging
import time

import httpx

from mcp_airflow.config import AirflowConfig

logger = logging.getLogger(__name__)

_config: AirflowConfig | None = None
_client: httpx.AsyncClient | None = None
_jwt_token: str | None = None
_jwt_expires_at: float = 0


def get_config() -> AirflowConfig:
    """Return cached config, loading from env on first call."""
    global _config
    if _config is None:
        _config = AirflowConfig.from_env()
    return _config


async def _obtain_jwt(base_url: str, username: str, password: str) -> str:
    """Obtain a JWT token from Airflow 3.x /auth/token endpoint."""
    global _jwt_token, _jwt_expires_at

    # base_url is e.g. http://host:8080/api/v2 — auth endpoint is at root
    from urllib.parse import urlparse

    parsed = urlparse(base_url)
    root_url = f"{parsed.scheme}://{parsed.netloc}"

    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(
            f"{root_url}/auth/token",
            json={"username": username, "password": password},
        )
        if r.status_code in (200, 201):
            data = r.json()
            _jwt_token = data["access_token"]
            _jwt_expires_at = time.time() + 3000  # refresh every ~50 min
            return _jwt_token

    msg = f"Failed to obtain JWT token: {r.status_code} {r.text[:200]}"
    raise RuntimeError(msg)


async def get_client() -> httpx.AsyncClient:
    """Return a shared httpx.AsyncClient.

    Tries JWT auth first (Airflow 3.x), falls back to basic auth (2.x).
    """
    global _client, _jwt_token

    config = get_config()

    if _client is not None and not _client.is_closed:
        # Refresh JWT if expired
        if _jwt_token and time.time() > _jwt_expires_at:
            _jwt_token = await _obtain_jwt(config.base_url, config.username, config.password)
            _client.headers["Authorization"] = f"Bearer {_jwt_token}"
        return _client

    # Try JWT auth first (Airflow 3.x)
    try:
        token = await _obtain_jwt(config.base_url, config.username, config.password)
        _client = httpx.AsyncClient(
            base_url=config.base_url,
            timeout=httpx.Timeout(30.0),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        logger.info("Airflow client using JWT auth (3.x)")
        return _client
    except Exception:
        logger.info("JWT auth failed, falling back to basic auth (2.x)")

    # Fallback to basic auth (Airflow 2.x)
    _client = httpx.AsyncClient(
        base_url=config.base_url,
        auth=httpx.BasicAuth(config.username, config.password),
        timeout=httpx.Timeout(30.0),
        headers={"Content-Type": "application/json"},
    )
    return _client


async def airflow_get(path: str, params: dict | None = None) -> dict:
    """Execute a GET request against the Airflow API."""
    client = await get_client()
    response = await client.get(path, params=params)
    response.raise_for_status()
    return response.json()


async def airflow_post(path: str, json_body: dict | None = None) -> dict:
    """Execute a POST request against the Airflow API."""
    client = await get_client()
    response = await client.post(path, json=json_body or {})
    response.raise_for_status()
    return response.json()

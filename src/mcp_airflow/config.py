"""Airflow connection configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AirflowConfig:
    """Configuration for connecting to an Airflow instance."""

    base_url: str
    username: str
    password: str

    @classmethod
    def from_env(cls) -> AirflowConfig:
        """Load configuration from environment variables.

        Required env vars:
            AIRFLOW_BASE_URL: Base URL of the Airflow REST API. Use /api/v2 for
                Airflow 3.x (e.g., http://100.x.x.x:8080/api/v2) or /api/v1 for 2.x.
            AIRFLOW_USERNAME: Auth username (JWT on 3.x, basic auth on 2.x)
            AIRFLOW_PASSWORD: Auth password
        """
        base_url = os.environ.get("AIRFLOW_BASE_URL", "")
        if not base_url:
            raise ValueError("AIRFLOW_BASE_URL environment variable is required")

        username = os.environ.get("AIRFLOW_USERNAME", "")
        if not username:
            raise ValueError("AIRFLOW_USERNAME environment variable is required")

        password = os.environ.get("AIRFLOW_PASSWORD", "")
        if not password:
            raise ValueError("AIRFLOW_PASSWORD environment variable is required")

        # Strip trailing slash for consistency
        return cls(
            base_url=base_url.rstrip("/"),
            username=username,
            password=password,
        )

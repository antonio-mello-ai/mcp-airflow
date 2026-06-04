# mcp-airflow

MCP server that exposes Apache Airflow REST API operations as tools. Built with [FastMCP](https://github.com/jlowin/fastmcp).

## Install

```bash
# Run directly with uvx (no install needed)
uvx mcp-airflow

# Or install with pip
pip install mcp-airflow
```

For development:

```bash
uv pip install -e ".[dev]"
# or with dependency groups
uv sync --group dev
```

## Configuration

Set these environment variables (or create a `.env` file from `.env.example`):

| Variable | Description | Example |
|----------|-------------|---------|
| `AIRFLOW_BASE_URL` | Airflow REST API base URL. Use `/api/v2` for Airflow 3.x or `/api/v1` for 2.x | `http://100.x.x.x:8080/api/v2` |
| `AIRFLOW_USERNAME` | Auth username (JWT on 3.x, basic auth on 2.x) | `admin` |
| `AIRFLOW_PASSWORD` | Auth password | |

### Authentication

The client picks the auth scheme automatically based on your Airflow version:

- **Airflow 3.x (JWT)** — a JWT token is obtained from the `/auth/token` endpoint
  using `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD`, sent as a `Bearer` token, and
  refreshed automatically. Point `AIRFLOW_BASE_URL` at `/api/v2`.
- **Airflow 2.x (basic auth)** — if the JWT flow is unavailable, the client falls
  back to HTTP basic auth with the same username/password. Point `AIRFLOW_BASE_URL`
  at `/api/v1`.

## Usage

Run the server:

```bash
mcp-airflow
```

Or add to your MCP client config (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "airflow": {
      "command": "mcp-airflow",
      "env": {
        "AIRFLOW_BASE_URL": "http://100.x.x.x:8080/api/v2",
        "AIRFLOW_USERNAME": "admin",
        "AIRFLOW_PASSWORD": "your-password"
      }
    }
  }
}
```

## Tools

| Tool | Description |
|------|-------------|
| `list_dags` | List all DAGs with paused/active status |
| `get_dag_runs_today` | Get all DAG runs from today with status |
| `get_dag_run_status` | Get the latest run status for a specific DAG |
| `trigger_dag_run` | Trigger a manual DAG run |
| `get_task_instances` | Get task instances for a specific DAG run |
| `check_failed_dags` | Check for failed DAGs in the last 24 hours |
| `check_scheduler_health` | Check scheduler heartbeat and metadatabase status |

## Tests

```bash
pytest
```

## License

MIT

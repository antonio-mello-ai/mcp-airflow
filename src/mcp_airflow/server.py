"""FastMCP server for Apache Airflow."""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("mcp-airflow")

# Import tool modules to register them with the server
from mcp_airflow.tools import dags, health, runs  # noqa: E402, F401


def main():
    """Entry point for the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()

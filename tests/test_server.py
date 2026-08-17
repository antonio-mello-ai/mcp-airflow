"""Smoke tests for the MCP server module.

The unit tests exercise the tool functions directly, so nothing else imports
``mcp_airflow.server``. This test makes sure the server module — and the MCP SDK
API it depends on — still imports and registers every tool. This is what
breaks when the ``mcp`` package ships an incompatible major version
(``mcp`` 2.0.0 removed ``mcp.server.fastmcp``).
"""

from __future__ import annotations

import asyncio

from mcp_airflow import server


def test_server_module_imports() -> None:
    assert server.mcp.name == "mcp-airflow"


def test_server_registers_tools() -> None:
    tools = asyncio.run(server.mcp.list_tools())
    assert len(tools) == 7

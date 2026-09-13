"""The one receiver-owned MCP operation available to Director agent turns.

The official low-level MCP server owns stdio framing and schema validation.
It deliberately avoids FastMCP settings, which load a cwd ``.env`` file and
would block on Director's owner-only 1Password FIFO mount.
"""
from __future__ import annotations

import argparse
import asyncio
from argparse import Namespace
import json
from pathlib import Path
from typing import Any

from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from .__main__ import load_config
from .runtime import database_path
from .service_queue import CommandUncertain, ReceiverUnavailable, ServiceCommandFailed, submit_command


PUBLISH_REPLY_SCHEMA = {
    "type": "object",
    "properties": {
        "text": {"type": "string"},
        "authority": {"type": "string"},
        "responsibility_id": {"type": ["string", "null"]},
        "execution_fence": {"type": ["string", "null"]},
        "conversation_title": {"type": ["string", "null"]},
        "conversation_emoji": {"type": ["string", "null"]},
        "conversation_preview": {"type": ["string", "null"]},
    },
    "required": ["text", "authority"],
    "additionalProperties": False,
}


def _result(payload: dict[str, object]) -> types.CallToolResult:
    """Return only safe publication state, never authority or source content."""
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(payload, separators=(",", ":")))],
        structuredContent=payload,
        isError=False,
    )


def publish_reply_result(
    path: Path,
    config: dict[str, Any],
    text: object,
    authority: object,
    responsibility_id: object = None,
    execution_fence: object = None,
    conversation_title: object = None,
    conversation_emoji: object = None,
    conversation_preview: object = None,
) -> dict[str, object]:
    """Submit one bounded receiver command; the dispatcher owns authority checks."""
    if not isinstance(authority, str) or not authority or len(authority) > 256:
        return {"published": False, "state": "authority_rejected"}
    if not isinstance(text, str) or not text.strip():
        return {"published": False, "state": "invalid_text"}
    if (responsibility_id is None) != (execution_fence is None):
        return {"published": False, "state": "responsibility_binding_invalid"}
    if responsibility_id is not None and (
        not isinstance(responsibility_id, str) or not isinstance(execution_fence, str)
    ):
        return {"published": False, "state": "responsibility_binding_invalid"}
    try:
        result = submit_command(
            path,
            Namespace(
                command="agent-publish",
                authority=authority,
                payload_text=text,
                responsibility_id=responsibility_id,
                fence=execution_fence,
                conversation_title=conversation_title,
                conversation_emoji=conversation_emoji,
                conversation_preview=conversation_preview,
            ),
            config,
            timeout=30,
        )
    except ReceiverUnavailable:
        return {"published": False, "state": "receiver_unavailable"}
    except CommandUncertain:
        return {"published": False, "state": "delivery_uncertain"}
    except ServiceCommandFailed:
        return {"published": False, "state": "receiver_rejected"}
    # The receiver result intentionally contains only publication state,
    # never source text, config data, credentials, or opaque authorities.
    return {"published": bool(result.get("published")), "state": result.get("state", "rejected")}


def build_server(config_path: Path) -> Server:
    """Build one channel-scoped stdio server without reading cwd dotenv files."""
    config = load_config(config_path)
    project = config_path.resolve().parent.parent
    path = database_path(config, project)
    server = Server("Director publish reply")

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="publish_reply",
                description="Publish this turn's reply after receiver-side source/fence validation.",
                inputSchema=PUBLISH_REPLY_SCHEMA,
            )
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, object]) -> types.CallToolResult:
        if name != "publish_reply":
            return _result({"published": False, "state": "tool_not_found"})
        return _result(
            await asyncio.to_thread(
                publish_reply_result,
                path,
                config,
                arguments.get("text"),
                arguments.get("authority"),
                arguments.get("responsibility_id"),
                arguments.get("execution_fence"),
                arguments.get("conversation_title"),
                arguments.get("conversation_emoji"),
                arguments.get("conversation_preview"),
            )
        )

    return server


async def _serve(server: Server) -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    asyncio.run(_serve(build_server(args.config.resolve())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

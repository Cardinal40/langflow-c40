# langflow-agent

MCP server giving Claude full agent access to Langflow: run the SecondDraft
interview pipeline (async — start + poll, immune to MCP timeouts), inspect and
modify any flow (nodes, prompts, wiring, custom components, auto-backups), with
built-in knowledge (`langflow_guide`, `pipeline_docs`). 19 tools.

## Mode A — hosted (zero install for users)  ← recommended
Run next to your deployed Langflow:

    MCP_TRANSPORT=http MCP_PORT=8765 MCP_PATH_SECRET=<long-random-string> \
    LANGFLOW_URL=http://localhost:7860 LANGFLOW_API_KEY=sk-... \
    uv run --script langflow_agent.py

(or `docker build -t langflow-agent . && docker run -e ... -p 8765:8765 langflow-agent`)

Expose it via your HTTPS reverse proxy, e.g.
`https://your-host/mcp-<secret>` → `localhost:8765/mcp-<secret>`.

Users then connect with NO installation: Claude → Settings → Connectors →
**Add custom connector** → paste the URL. Works in Claude Desktop and claude.ai.
The Langflow API key stays server-side; the unguessable URL path is the access
control — treat the URL as a secret, HTTPS only.

## Mode B — local (Claude Desktop launches it)
Config entry: command `uv`, args `["run", "--script", "<path>/langflow_agent.py"]`,
env `LANGFLOW_URL`, `LANGFLOW_API_KEY`, `FLOW_ID`. Default transport is stdio.

## Long runs
`create_interview_map` / `get_next_question` return a run id immediately;
Claude polls `get_run_status` — no MCP client timeout can interrupt a pipeline
run of any length.

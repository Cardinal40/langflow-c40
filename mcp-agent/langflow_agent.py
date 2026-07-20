# /// script
# requires-python = ">=3.11"
# dependencies = ["mcp>=1.2.0", "httpx>=0.27"]
# ///
"""MCP server: Claude as a full agent over Langflow.

Lets Claude Desktop RUN the SecondDraft interview pipeline with proper named
inputs, and INSPECT + MODIFY Langflow workflows natively (nodes, prompts,
wiring, custom components) — with automatic backups, plus built-in knowledge:
langflow_guide() for Langflow's component model and pipeline_docs() for how
the interview pipeline is engineered.

Config via env vars: LANGFLOW_URL, LANGFLOW_API_KEY, FLOW_ID (default flow).
"""

import copy
import hashlib
import json
import os
import re
import threading
import time
import uuid
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

LANGFLOW_URL = os.environ.get("LANGFLOW_URL", "http://localhost:7860").rstrip("/")
API_KEY = os.environ.get("LANGFLOW_API_KEY", "")
FLOW_ID = os.environ.get("FLOW_ID", "3b03fd6f-991f-4d47-9e0c-6629d34c1fa6")
BACKUP_DIR = Path(os.environ.get("BACKUP_DIR", str(Path(__file__).parent / "backups")))

# Node ids inside the default interview flow.
NODE_DRAFT = "TextInput-l3WGO"
NODE_GOAL = "TextInput-geJay"
NODE_STYLE = "TextInput-qcecn"
NODE_PRIOR_QAS = "TextInput-priqa"
NODE_PUCT = "PUCTSelector-p40ct"
OUT_MAP = "TextOutput-TFJa3"
OUT_QUESTION = "TextOutput-nextq"
OUT_SCOREBOARD = "TextOutput-puctb"

mcp = FastMCP(
    "langflow-agent",
    instructions=(
        "Full agent access to the Langflow server hosting the SecondDraft interview "
        "pipeline. You can RUN the pipeline (create_interview_map / get_next_question "
        "take proper named inputs) and INSPECT + MODIFY any workflow (nodes, prompts, "
        "wiring, custom Python components) — never suggest the browser UI for edits. "
        "Before your first edit in a conversation, call langflow_guide() for Langflow's "
        "component model and pipeline_docs() for how this pipeline is engineered — "
        "follow its design principles when changing it. Every mutation auto-saves a "
        "backup; restore_backup undoes mistakes. Interview workflow: create_interview_map "
        "with the draft (STARTS a background run — poll get_run_status for the map), "
        "then repeat get_next_question with all answers so far (one line each: "
        "'[Branch Label] Q: ... A: ...'), polling get_run_status each time, until it "
        "reports completion. Long runs never block a tool call, so no MCP timeouts."
    ),
)

# ── HTTP helpers ──────────────────────────────────────────────────────────────


def _client() -> httpx.Client:
    if not API_KEY:
        raise RuntimeError("LANGFLOW_API_KEY is not set — check the Claude Desktop config.")
    return httpx.Client(
        base_url=LANGFLOW_URL,
        headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
        timeout=httpx.Timeout(900.0, connect=15.0),
    )


def _check(r: httpx.Response) -> dict | list:
    if r.status_code >= 400:
        raise RuntimeError(f"Langflow returned {r.status_code}: {r.text[:500]}")
    return r.json()


def _get_flow(c: httpx.Client, flow_id: str) -> dict:
    return _check(c.get(f"/api/v1/flows/{flow_id}"))


def _save_flow(c: httpx.Client, flow_id: str, flow: dict) -> None:
    _backup(flow)
    _check(c.patch(f"/api/v1/flows/{flow_id}", json={"data": flow["data"]}))


def _backup(flow: dict) -> str:
    BACKUP_DIR.mkdir(exist_ok=True)
    name = f"{flow['name'][:40].replace('/', '_')}-{time.strftime('%Y%m%d-%H%M%S')}.json"
    (BACKUP_DIR / name).write_text(json.dumps({"id": flow["id"], "name": flow["name"], "data": flow["data"]}))
    return name


def _node(flow: dict, node_id: str) -> dict:
    for n in flow["data"]["nodes"]:
        if n["id"] == node_id:
            return n
    ids = [n["id"] for n in flow["data"]["nodes"]]
    raise RuntimeError(f"Node '{node_id}' not found. Nodes: {ids}")


def _esc(o: dict) -> str:
    # Langflow's frontend escapes edge-handle JSON with 'œ' instead of '"'.
    return json.dumps(o, separators=(",", ":")).replace('"', "œ")


def _components(c: httpx.Client) -> dict:
    return _check(c.get("/api/v1/all"))


# ── Prompt Template variable sync ────────────────────────────────────────────

_VAR_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

_VAR_FIELD_PROTO = {
    "field_type": "str",
    "required": False,
    "placeholder": "",
    "list": False,
    "show": True,
    "multiline": True,
    "fileTypes": [],
    "file_path": "",
    "advanced": False,
    "input_types": ["Message"],
    "dynamic": False,
    "info": "",
    "load_from_db": False,
    "title_case": False,
    "type": "str",
    "value": "",
}


def _sync_prompt_vars(node: dict) -> list[str]:
    """After editing a Prompt Template's text, align its variable fields."""
    nd = node["data"]["node"]
    tpl = nd["template"]
    text = tpl.get("template", {}).get("value", "")
    wanted = list(dict.fromkeys(_VAR_RE.findall(text)))
    current = nd.get("custom_fields", {}).get("template", [])
    for v in wanted:
        if v not in tpl:
            f = copy.deepcopy(_VAR_FIELD_PROTO)
            f["name"] = f["display_name"] = v
            tpl[v] = f
    for v in current:
        if v not in wanted and v in tpl:
            del tpl[v]
    nd.setdefault("custom_fields", {})["template"] = wanted
    return wanted


# ── Run tools ─────────────────────────────────────────────────────────────────

# Long pipeline runs exceed MCP client timeouts (~60s), so the interview tools
# START a run in a background thread and return a run id; get_run_status polls.
_RUNS: dict[str, dict] = {}


def _start_background(flow_id: str, tweaks: dict, session_key: str, kind: str) -> str:
    run_id = uuid.uuid4().hex[:8]
    _RUNS[run_id] = {"status": "running", "kind": kind, "started": time.time()}

    def work():
        try:
            results = _run(flow_id, tweaks, session_key)
            _RUNS[run_id].update(status="done", results=results)
        except Exception as e:  # noqa: BLE001 — surfaced to the caller via status
            _RUNS[run_id].update(status="error", error=str(e)[:600])

    threading.Thread(target=work, daemon=True).start()
    return run_id


def _format_results(kind: str, results: dict[str, str]) -> str:
    if kind == "map":
        return results.get(OUT_MAP) or f"(no map output; components: {list(results)})"
    if kind == "turn":
        return (
            f"NEXT QUESTION:\n{results.get(OUT_QUESTION, '(none produced)')}\n\n"
            f"PUCT SCOREBOARD:\n{results.get(OUT_SCOREBOARD, '(none produced)')}"
        )
    return "\n\n".join(f"── {cid} ──\n{text}" for cid, text in results.items())


def _run(flow_id: str, tweaks: dict, session_key: str) -> dict[str, str]:
    payload = {
        "output_type": "debug",
        "input_type": "text",
        "tweaks": tweaks,
        # stable session id → Langflow reuses cached results for unchanged components
        "session_id": "mcp-" + hashlib.sha256(session_key.encode()).hexdigest()[:16],
    }
    with _client() as c:
        data = _check(c.post(f"/api/v1/run/{flow_id}", params={"stream": "false"}, json=payload))
    results: dict[str, str] = {}
    for run in data.get("outputs", []):
        for out in run.get("outputs", []):
            texts = []
            for v in (out.get("results") or {}).values():
                if isinstance(v, dict):
                    t = v.get("text") or (v.get("data") or {}).get("text")
                    if isinstance(t, dict):
                        t = t.get("text")
                    if t:
                        texts.append(str(t))
                elif isinstance(v, str):
                    texts.append(v)
            if texts:
                results[out.get("component_id", "")] = "\n".join(texts)
    if not results:
        raise RuntimeError(f"Flow ran but returned no outputs. Raw keys: {list(data.keys())}")
    return results


@mcp.tool()
def create_interview_map(draft: str, goal: str = "", style_guide: str = "") -> str:
    """START an interview-map run on a draft (returns immediately with a run id —
    the full editorial pipeline takes several minutes, far beyond MCP tool
    timeouts, so poll get_run_status for the result: ranked conversation
    branches, each with a label, description, and opening question).

    Args:
        draft: The writer's full raw draft text (required).
        goal: One or two sentences on what the writer wants the piece to achieve.
        style_guide: The writer's style guide, if available.
    """
    tweaks = {
        NODE_DRAFT: {"input_value": draft},
        NODE_GOAL: {"input_value": goal},
        NODE_STYLE: {"input_value": style_guide},
        NODE_PRIOR_QAS: {"input_value": ""},
    }
    run_id = _start_background(FLOW_ID, tweaks, session_key=draft, kind="map")
    return (
        f"Run {run_id} started (full planning pipeline, typically 3-10 minutes). "
        f"Poll get_run_status('{run_id}') every ~30-60 seconds until it returns the map. "
        "Tell the user the pipeline is running; you can do other work meanwhile."
    )


@mcp.tool()
def get_next_question(
    draft: str,
    prior_qas: str,
    goal: str = "",
    style_guide: str = "",
    current_branch: str = "",
    last_richness: float = 0.0,
) -> str:
    """START a next-question run (returns immediately with a run id; poll
    get_run_status). The result is the PUCT-selected next question plus the
    scoreboard explaining the choice, or interview completion once the
    readiness threshold is reached.

    Args:
        draft: The writer's full raw draft (same as used for the map).
        prior_qas: The interview so far, ONE LINE PER EXCHANGE, formatted exactly:
            [Branch Label] Q: <question> A: <answer>   ("A: (skipped)" for skips)
        goal: The writer's goal (same as used for the map).
        style_guide: The style guide (same as used for the map).
        current_branch: Label of the branch just answered (drives momentum).
        last_richness: 0-1, how substantive the last answer was.
    """
    tweaks = {
        NODE_DRAFT: {"input_value": draft},
        NODE_GOAL: {"input_value": goal},
        NODE_STYLE: {"input_value": style_guide},
        NODE_PRIOR_QAS: {"input_value": prior_qas},
        NODE_PUCT: {"current_branch": current_branch, "last_richness": last_richness},
    }
    run_id = _start_background(FLOW_ID, tweaks, session_key=draft, kind="turn")
    return (
        f"Run {run_id} started (usually under a minute on repeat turns thanks to "
        f"caching, but can take longer). Poll get_run_status('{run_id}') until done."
    )


@mcp.tool()
def get_run_status(run_id: str) -> str:
    """Check a pipeline run started by create_interview_map or get_next_question.
    Returns 'running' with elapsed time, an error, or the finished result.
    Poll every ~30-60 seconds while a run is in progress.
    """
    r = _RUNS.get(run_id)
    if not r:
        known = list(_RUNS)
        return f"Unknown run id '{run_id}'. Known runs this session: {known or 'none'}."
    if r["status"] == "running":
        return f"Run {run_id} still running ({int(time.time() - r['started'])}s elapsed). Poll again shortly."
    if r["status"] == "error":
        return f"Run {run_id} FAILED: {r['error']}"
    return _format_results(r["kind"], r["results"])


@mcp.tool()
def run_flow(flow_id: str = "", tweaks: str = "{}", session_key: str = "default") -> str:
    """Run any flow SYNCHRONOUSLY and return every output component's text. Use
    for quick tests after edits (small inputs / cached stages). For anything
    that may exceed ~50 seconds, use create_interview_map / get_next_question
    (async) instead, or keep test inputs tiny.

    Args:
        flow_id: Flow UUID (empty = the interview pipeline flow).
        tweaks: JSON object mapping node id -> {field: value}, e.g.
            {"TextInput-abc": {"input_value": "hello"}}.
        session_key: Any string; runs with the same key reuse cached component
            results when inputs are unchanged.
    """
    results = _run(flow_id or FLOW_ID, json.loads(tweaks), session_key)
    return "\n\n".join(f"── {cid} ──\n{text}" for cid, text in results.items())


# ── Inspection tools ──────────────────────────────────────────────────────────


@mcp.tool()
def list_flows() -> str:
    """List all flows on the Langflow server (id, name, last updated)."""
    with _client() as c:
        flows = _check(c.get("/api/v1/flows/", params={"get_all": "true", "header_flows": "true"}))
    lines = [f"{f['id']}  |  {f['name']}  |  updated {f.get('updated_at', '?')}" for f in flows]
    return "\n".join(lines) or "(no flows)"


@mcp.tool()
def describe_flow(flow_id: str = "") -> str:
    """Show a flow's structure: every node (id, component type, display name) and
    every edge in readable form. Start here before editing.

    Args:
        flow_id: Flow UUID (empty = the interview pipeline flow).
    """
    with _client() as c:
        flow = _get_flow(c, flow_id or FLOW_ID)
    nodes = flow["data"]["nodes"]
    names = {n["id"]: n["data"].get("node", {}).get("display_name", n["id"]) for n in nodes}
    out = [f"FLOW: {flow['name']} ({flow['id']})", f"\nNODES ({len(nodes)}):"]
    for n in nodes:
        nd = n["data"]
        out.append(f'  {n["id"]:28} type={nd.get("type", "?"):22} "{names[n["id"]]}"')
    out.append(f"\nEDGES ({len(flow['data']['edges'])}):")
    for e in flow["data"]["edges"]:
        sh, th = e["data"]["sourceHandle"], e["data"]["targetHandle"]
        out.append(
            f"  {names.get(e['source'], e['source'])} [{sh.get('name')}] -> "
            f"{names.get(e['target'], e['target'])} [{th.get('fieldName')}]"
        )
    return "\n".join(out)


@mcp.tool()
def inspect_node(node_id: str, flow_id: str = "", full_field: str = "") -> str:
    """Show a node's editable fields with their current values (long values are
    truncated), plus its output ports.

    Args:
        node_id: The node id (from describe_flow).
        flow_id: Flow UUID (empty = the interview pipeline flow).
        full_field: Name of one field to show untruncated (e.g. "system_prompt").
    """
    with _client() as c:
        flow = _get_flow(c, flow_id or FLOW_ID)
    node = _node(flow, node_id)
    nd = node["data"]["node"]
    out = [f'NODE {node_id} — "{nd.get("display_name")}" (type={node["data"].get("type")})']
    out.append(f"description: {nd.get('description', '')}")
    out.append("\nFIELDS:")
    for name, f in nd.get("template", {}).items():
        if name.startswith("_") or name == "code" or not isinstance(f, dict):
            continue
        val = f.get("value")
        s = json.dumps(val, default=str) if not isinstance(val, str) else val
        if name != full_field and s and len(s) > 200:
            s = s[:200] + f"... [{len(s)} chars — pass full_field='{name}' to see all]"
        out.append(f"  {name} ({f.get('type', '?')}): {s}")
    outs = [(o.get("name"), o.get("types")) for o in nd.get("outputs", [])]
    out.append(f"\nOUTPUT PORTS: {outs}")
    return "\n".join(out)


@mcp.tool()
def list_components(search: str = "") -> str:
    """Search Langflow's component catalog (~726 components: models, agents,
    prompts, vector stores, tools, logic, I/O ...). Returns matching component
    type names usable with add_node.

    Args:
        search: Case-insensitive substring matched against component name,
            category, and description. Empty = list category names only.
    """
    with _client() as c:
        idx = _components(c)
    if not search:
        return "CATEGORIES:\n" + "\n".join(f"  {cat} ({len(comps)})" for cat, comps in sorted(idx.items()))
    q = search.lower()
    hits = []
    for cat, comps in idx.items():
        for name, comp in comps.items():
            desc = comp.get("description") if isinstance(comp, dict) else None
            desc = desc if isinstance(desc, str) else ""
            if q in name.lower() or q in cat.lower() or q in desc.lower():
                hits.append(f"  {name}  [{cat}]  — {desc[:100]}")
    return f"MATCHES ({len(hits)}):\n" + "\n".join(hits[:60]) if hits else "No matches."


# ── Mutation tools ────────────────────────────────────────────────────────────


@mcp.tool()
def create_flow(name: str, description: str = "") -> str:
    """Create a new empty flow. Returns its UUID."""
    with _client() as c:
        flow = _check(
            c.post(
                "/api/v1/flows/",
                json={
                    "name": name,
                    "description": description,
                    "data": {"nodes": [], "edges": []},
                },
            )
        )
    return f"Created flow '{flow['name']}' with id {flow['id']}"


@mcp.tool()
def set_node_fields(node_id: str, fields: str, flow_id: str = "") -> str:
    """Set field values on a node (system prompts, template text, model names,
    tunables ...). On Prompt Template nodes, editing the 'template' field
    automatically syncs the {variable} input ports.

    Args:
        node_id: The node id.
        fields: JSON object of {field_name: value}, e.g.
            {"system_prompt": "You are ...", "top_k": 5}.
        flow_id: Flow UUID (empty = the interview pipeline flow).
    """
    updates = json.loads(fields)
    with _client() as c:
        flow = _get_flow(c, flow_id or FLOW_ID)
        node = _node(flow, node_id)
        tpl = node["data"]["node"]["template"]
        applied, synced = [], None
        for name, value in updates.items():
            if name not in tpl:
                raise RuntimeError(f"Field '{name}' not on node. Fields: {[k for k in tpl if not k.startswith('_')]}")
            tpl[name]["value"] = value
            applied.append(name)
        if "template" in updates:
            synced = _sync_prompt_vars(node)
        _save_flow(c, flow["id"], flow)
    msg = f"Updated {applied} on {node_id}."
    if synced is not None:
        msg += f" Prompt variables synced to: {synced} (rewire edges if variables changed)."
    return msg


@mcp.tool()
def add_node(
    component_type: str, x: float, y: float, flow_id: str = "", display_name: str = "", fields: str = "{}"
) -> str:
    """Add a component from the catalog to a flow. Returns the new node id.

    Args:
        component_type: Exact component name from list_components (e.g. "TextInput",
            "Agent", "Prompt Template", "TextOutput").
        x, y: Canvas position (pick coordinates near related nodes).
        flow_id: Flow UUID (empty = the interview pipeline flow).
        display_name: Optional custom display name.
        fields: Optional JSON of initial field values, e.g. {"input_value": "hi"}.
    """
    with _client() as c:
        idx = _components(c)
        template = None
        for comps in idx.values():
            if component_type in comps:
                template = copy.deepcopy(comps[component_type])
                break
        if template is None:
            raise RuntimeError(f"Component '{component_type}' not found — use list_components to search.")
        new_id = f"{component_type}-{hashlib.sha256(os.urandom(8)).hexdigest()[:5]}"
        if display_name:
            template["display_name"] = display_name
        for name, value in json.loads(fields).items():
            if name in template.get("template", {}):
                template["template"][name]["value"] = value
        node = {
            "id": new_id,
            "type": "genericNode",
            "position": {"x": x, "y": y},
            "data": {"type": component_type, "id": new_id, "node": template},
        }
        flow = _get_flow(c, flow_id or FLOW_ID)
        flow["data"]["nodes"].append(node)
        _save_flow(c, flow["id"], flow)
    return f"Added node {new_id} ({component_type})."


@mcp.tool()
def add_custom_component(code: str, x: float, y: float, flow_id: str = "") -> str:
    """Add a custom Python component to a flow. The code must define a class
    extending langflow's Component with `inputs`, `outputs`, and output methods
    (see langflow_guide for the pattern). Langflow validates the code first.

    Args:
        code: Full Python source of the component.
        x, y: Canvas position.
        flow_id: Flow UUID (empty = the interview pipeline flow).
    """
    with _client() as c:
        built = _check(c.post("/api/v1/custom_component", json={"code": code}))
        comp_type = built["type"]
        new_id = f"{comp_type}-{hashlib.sha256(os.urandom(8)).hexdigest()[:5]}"
        node = {
            "id": new_id,
            "type": "genericNode",
            "position": {"x": x, "y": y},
            "data": {"type": comp_type, "id": new_id, "node": built["data"]},
        }
        flow = _get_flow(c, flow_id or FLOW_ID)
        flow["data"]["nodes"].append(node)
        _save_flow(c, flow["id"], flow)
    return f"Added custom component node {new_id} (type {comp_type})."


@mcp.tool()
def connect_nodes(source_id: str, output_name: str, target_id: str, field_name: str, flow_id: str = "") -> str:
    """Draw an edge: a source node's output port into a target node's input field.
    Handle encoding and type metadata are managed automatically.

    Args:
        source_id: Source node id.
        output_name: Output port name on the source (see inspect_node OUTPUT
            PORTS; e.g. "text", "response", "prompt", "message",
            "component_as_tool" for wiring an agent's tools).
        target_id: Target node id.
        field_name: Input field on the target (e.g. "input_value", a prompt
            variable name, "tools" for agent tool wiring).
        flow_id: Flow UUID (empty = the interview pipeline flow).
    """
    with _client() as c:
        flow = _get_flow(c, flow_id or FLOW_ID)
        src, tgt = _node(flow, source_id), _node(flow, target_id)
        outputs = {o.get("name"): o for o in src["data"]["node"].get("outputs", [])}
        if output_name not in outputs:
            raise RuntimeError(f"Output '{output_name}' not on {source_id}. Outputs: {list(outputs)}")
        tpl = tgt["data"]["node"]["template"]
        if field_name not in tpl:
            raise RuntimeError(
                f"Field '{field_name}' not on {target_id}. Fields: {[k for k in tpl if not k.startswith('_')]}"
            )
        out_types = outputs[output_name].get("types") or ["Message"]
        in_types = tpl[field_name].get("input_types") or ["Message"]
        e = {
            "source": source_id,
            "target": target_id,
            "data": {
                "sourceHandle": {
                    "dataType": src["data"].get("type"),
                    "id": source_id,
                    "name": output_name,
                    "output_types": out_types,
                },
                "targetHandle": {
                    "fieldName": field_name,
                    "id": target_id,
                    "inputTypes": in_types,
                    "type": tpl[field_name].get("type", "str"),
                },
            },
        }
        e["sourceHandle"] = _esc(e["data"]["sourceHandle"])
        e["targetHandle"] = _esc(e["data"]["targetHandle"])
        e["id"] = "reactflow__edge-" + source_id + e["sourceHandle"] + "-" + target_id + e["targetHandle"]
        flow["data"]["edges"].append(e)
        _save_flow(c, flow["id"], flow)
    return f"Connected {source_id}[{output_name}] -> {target_id}[{field_name}]."


@mcp.tool()
def disconnect_nodes(source_id: str, target_id: str, field_name: str = "", flow_id: str = "") -> str:
    """Remove edge(s) between two nodes (optionally only for one target field)."""
    with _client() as c:
        flow = _get_flow(c, flow_id or FLOW_ID)
        before = len(flow["data"]["edges"])
        flow["data"]["edges"] = [
            e
            for e in flow["data"]["edges"]
            if not (
                e["source"] == source_id
                and e["target"] == target_id
                and (not field_name or e["data"]["targetHandle"].get("fieldName") == field_name)
            )
        ]
        removed = before - len(flow["data"]["edges"])
        if not removed:
            raise RuntimeError("No matching edges found.")
        _save_flow(c, flow["id"], flow)
    return f"Removed {removed} edge(s)."


@mcp.tool()
def delete_node(node_id: str, flow_id: str = "") -> str:
    """Delete a node and every edge attached to it."""
    with _client() as c:
        flow = _get_flow(c, flow_id or FLOW_ID)
        _node(flow, node_id)  # existence check
        flow["data"]["nodes"] = [n for n in flow["data"]["nodes"] if n["id"] != node_id]
        n_edges = len(flow["data"]["edges"])
        flow["data"]["edges"] = [e for e in flow["data"]["edges"] if node_id not in (e["source"], e["target"])]
        _save_flow(c, flow["id"], flow)
    return f"Deleted {node_id} and {n_edges - len(flow['data']['edges'])} attached edge(s)."


@mcp.tool()
def list_backups() -> str:
    """List flow backups (created automatically before every mutation)."""
    if not BACKUP_DIR.exists():
        return "(no backups yet)"
    files = sorted(BACKUP_DIR.glob("*.json"), reverse=True)
    return "\n".join(f.name for f in files[:30]) or "(no backups yet)"


@mcp.tool()
def restore_backup(backup_name: str) -> str:
    """Restore a flow to a backed-up state (from list_backups). Undoes edits."""
    snap = json.loads((BACKUP_DIR / backup_name).read_text())
    with _client() as c:
        _check(c.patch(f"/api/v1/flows/{snap['id']}", json={"data": snap["data"]}))
    return f"Restored flow '{snap['name']}' ({snap['id']}) from {backup_name}."


# ── Knowledge ─────────────────────────────────────────────────────────────────


@mcp.tool()
def langflow_guide() -> str:
    """Langflow's component model, design patterns, and editing rules.
    Call this before making flow edits.
    """
    return GUIDE


@mcp.tool()
def pipeline_docs() -> str:
    """Engineering documentation of the interview pipeline: architecture,
    provenance (the production Go/Langfuse system it ports), the PUCT math,
    prompt lineage, tunables, and the design principles to follow when
    modifying it. Call this before changing the pipeline.
    """
    return PIPELINE_DOCS


GUIDE = """\
LANGFLOW — COMPONENT MODEL AND DESIGN PATTERNS (for flow editing via these tools)

CORE MODEL
- A flow is a DAG: nodes (component instances) + edges (data connections).
- Data flows between nodes as Message objects (text + metadata). An edge connects
  a source node's OUTPUT PORT to a target node's INPUT FIELD.
- Components come from a catalog of ~726 types (list_components). Key families:
  * I/O: TextInput/TextOutput (canvas fields; settable via run tweaks),
    ChatInput/ChatOutput (Playground chat).
  * Prompt Template: text with {variables}; each variable becomes an input field
    you can wire an edge into. Editing the text via set_node_fields auto-syncs
    the variable fields.
  * Agent: LLM with system prompt (field: system_prompt), model selection, an
    input_value (usually fed by a Prompt Template), and optional TOOLS — wire
    another component's 'component_as_tool' output into the agent's 'tools'
    field and the agent can call it mid-run.
  * Custom Component: a Python class extending Component with declared inputs
    (MessageTextInput, FloatInput, IntInput, StrInput, SecretStrInput,
    MultilineInput) and Output(name=..., method=...) entries; each output method
    returns Message(text=...). Use for deterministic logic — math, parsing,
    API calls — never prompt an LLM to do arithmetic.
- Running: the run API executes the DAG for all outputs. TextInput values can be
  overridden per-run via tweaks {node_id: {field: value}} without editing the
  flow. Runs with the same session reuse cached results of components whose
  inputs did not change — that is what makes per-turn re-runs cheap.

EDITING RULES
1. Read before writing: describe_flow, then inspect_node on anything you touch.
2. Never hand-craft edges or edge JSON — use connect_nodes (it handles
   Langflow's œ-escaped handle encoding and type metadata).
3. Every mutation auto-saves a backup; if a change misbehaves, restore_backup.
4. Test after editing: run_flow with small inputs; read the output per node.
5. If a human has the flow open in the Langflow browser canvas while you edit,
   their stale canvas can overwrite your changes on their next interaction.
   Ask them to close or refresh the flow tab before/after editing sessions.
6. CAUTION: text with literal {braces} (JSON examples!) inside a Prompt Template
   is parsed as variables — put brace-heavy text in an agent's system_prompt.
7. Agent model fields expect the provider's model list format — change models by
   copying the value shape from an existing agent node
   (inspect_node full_field='model').
"""

PIPELINE_DOCS = """\
THE INTERVIEW PIPELINE — ENGINEERING DOCUMENTATION

WHAT IT IS AND WHERE IT CAME FROM
This flow ("Interview Pipeline + PUCT loop") is an experimentation port of the
SecondDraft production interview system. In production the pipeline runs inside
a Go middleware (repo: mvp2-middleware); its prompts live in Langfuse
(namespaces interview/v3 and interview/v4); a control plane called Goldberg
tunes params/order at runtime. This Langflow port mirrors those semantics so
experiments here predict production behavior. Serhii engineered it in stages,
and the method matters more than the artifact — see DESIGN PRINCIPLES.

ARCHITECTURE (two halves)
1) PLANNING (runs once per draft):
   Seeds: draft (TextInput-l3WGO), goal (TextInput-geJay),
   style guide (TextInput-qcecn), orientation override (TextInput-orovr),
   prior Q&As (TextInput-priqa).
   Chain: Orientation Questions (best-guess HOW_MUCH_WRITTEN + LATITUDE 1-100;
   writer can override via the override seed, which OUTRANKS the agent's guess)
   -> Big Idea Read-back -> Master Ghostwriter (Agent with the five persona
   agents wired in as TOOLS: Structural Editor, Evidence Interrogator, Voice
   Reader, Skeptical Audience, Researcher; plus Library Search exemplars)
   -> Bidding Round (reconciles findings into ranked flags)
   -> Branch Generator -> Interview Map (TextOutput-TFJa3), emitted as
   <BRANCHES> XML: each branch has LABEL / DESCRIPTION / QUESTION, ordered by
   importance (rank order becomes the PUCT prior).
2) TURN LOOP (runs once per answered question; state lives OUTSIDE the flow in
   the prior-Q&As log, so the flow itself is stateless):
   Coverage Input -> Coverage Scorer (scores each branch 0-1 from the answers,
   returns {label: score} JSON) -> PUCT Selector (PUCTSelector-p40ct, custom
   Python component) -> Follow-up Input -> Follow-up Question agent ->
   Next Question (TextOutput-nextq) + PUCT Scoreboard (TextOutput-puctb).

THE PUCT SELECTOR (deterministic code, not a prompt — by design)
Line-for-line port of the Go selection logic
(mvp2-middleware/services/chats/interview_tree.go, SelectNextBranch):
  U(b) = P(b)*(1 - cov(b))                      # remaining leverage
       + c * P(b) * sqrt(N) / (1 + n(b))        # curiosity
       + momentum (only on the just-answered branch:
         momentum_weight * richness * (1 - cov))
Priors come from map rank via RankWeights {1:0.30, 2:0.25, 3:0.20, 4:0.15,
5:0.10}. Closure rules: coverage >= PUCT_COVERAGE_CLOSE with >=1 answered;
2 skips; depth 3 (visit count proxy). Readiness = rank-weighted mean coverage
over open + covered-closed branches; interview ends at PUCT_READINESS.
Coverage fallback heuristic when the scorer omits a branch: each answer closes
half the remainder; skips multiply by 0.85.
Tunables on the node (production defaults): PUCT_EXPLORATION 1.0 (0.3-0.5
drains a branch before moving on; higher tours the pool), PUCT_MOMENTUM 0.2,
PUCT_COVERAGE_CLOSE 0.8, PUCT_READINESS 0.5. The port was verified against the
Go test scenarios before wiring in.

LIBRARY SEARCH (custom component, ports services/library/service.go)
draft (first 6000 chars) -> purpose description (gpt-4o, documentPurposePrompt
fallback text, editable on the node) -> OpenAI text-embedding-3-large at 1024
dims (must match the index) -> Pinecone index "burlesque-2", namespace
"purpose", top_k = EXEMPLAR_TOP_K (default 3) -> <LIBRARY_EXAMPLES> XML with
title/author/type/similarity/purpose/curator note/document text. doc_text_chars
(default 2000) truncates each document at a word boundary with an explicit
marker; 0 = full text (production behavior). Feeds Ghostwriter Input and
Follow-up Input. Keys are bound to Langflow global variables OPENAI_API_KEY and
LIBRARY_PINECONE_API_KEY.

PROMPT LINEAGE (system prompts are SNAPSHOTS of production Langfuse prompts)
Resolved the way the Go runtime assembles them: shared rule blocks spliced in
front, manifest headers stripped. Sources:
  Orientation Questions  <- interview/v3/agent/orientation v7
  Big Idea Read-back     <- interview/v3/agent/big-idea-chat v20
  Master Ghostwriter     <- interview/v4/ghostwriter v4
  five personas          <- interview/v4/agent/* v4 each
  Bidding Round          <- interview/v4/bidding v1
  Branch Generator       <- interview/v4/controllers/branch-generator v1
  Coverage Scorer        <- interview/v2/follow-up/coverage v7
  Follow-up Question     <- interview/v3/follow-up/question v4
Rules resolved into them: interview/v3/cache/rules/ryan-methodology v2,
lock-command v1, anchors v1, suggested-move v5 ("I am Ryan Jacobs..." prefix).
question-shape and taxonomy are literal "SKIPPED" stubs in Langfuse — omitted.
These snapshots do NOT stay live-linked to Langfuse; re-sync deliberately.

KNOWN DEVIATIONS FROM PRODUCTION
- Priors: rank order via RankWeights (production reranks priors dynamically).
- Depth is approximated by visit count.
- Models are hardcoded per node (production routes via Goldberg stage config):
  orientation sonnet-4-6, personas sonnet-5, main chain opus-4-8.
- Exemplars may be truncated (doc_text_chars); production sends full documents.
- The human-in-the-loop confirmation UI of production is replaced by the
  orientation-override seed and the external Q&A log.

DESIGN PRINCIPLES (how it was engineered — follow these when changing it)
1. Mirror production semantics faithfully; when you must deviate, document it.
2. Prompts are snapshots with provenance; methodology lives in system prompts,
   per-run data in Prompt Templates.
3. Deterministic logic (selection math, retrieval plumbing) is Python code in
   custom components — never ask an LLM to do arithmetic or API calls.
4. Every experiment knob is a node field with the production default and the
   Goldberg name (PUCT_EXPLORATION etc.), so results map back to production.
5. State stays outside the flow; each run is reproducible from its inputs.
6. Test changes with cheap runs (run_flow with tiny inputs) before real drafts;
   rely on session caching so unchanged stages don't re-execute.
7. Backups before every mutation; restore rather than debug a broken flow.
"""


if __name__ == "__main__":
    # Two ways to run:
    #   stdio (default)      — launched by Claude Desktop locally.
    #   streamable-http      — hosted next to Langflow; users connect with a URL
    #                          via Claude Settings -> Connectors (zero install).
    #                          Env: MCP_TRANSPORT=http, MCP_HOST, MCP_PORT,
    #                          MCP_PATH_SECRET (unguessable URL path segment).
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport in ("http", "streamable-http", "sse"):
        mcp.settings.host = os.environ.get("MCP_HOST", "0.0.0.0")
        mcp.settings.port = int(os.environ.get("MCP_PORT") or os.environ.get("PORT") or "8765")
        secret = os.environ.get("MCP_PATH_SECRET", "")
        if secret:
            mcp.settings.streamable_http_path = f"/mcp-{secret}"
        mcp.run(transport="streamable-http")
    else:
        mcp.run()

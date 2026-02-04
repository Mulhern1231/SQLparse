"""Exporters for lineage graphs."""

from __future__ import annotations

import json
from typing import Dict, Iterable, List


def export_graph(graph: Dict[str, object], format: str = "json") -> str:
    """Export a graph to the requested format."""

    normalized_format = format.lower()
    mode = graph.get("mode", "full")
    if normalized_format == "json":
        return json.dumps(graph, indent=2, ensure_ascii=False)
    if normalized_format == "mermaid_flowchart":
        return _export_mermaid_flowchart(graph)
    if normalized_format == "mermaid_er":
        if mode not in {"er_columns", "tables_only"}:
            _append_error(
                graph,
                "Mermaid ER export is only supported for er_columns or tables_only modes.",
            )
            return _export_mermaid_flowchart(graph)
        return _export_mermaid_er(graph)
    if normalized_format == "graphviz_dot":
        return _export_graphviz_dot(graph)
    _append_error(graph, f"Unsupported export format: {format}")
    return _export_mermaid_flowchart(graph)


def _append_error(graph: Dict[str, object], message: str) -> None:
    """Append an error message to the graph."""

    errors = graph.setdefault("errors", [])
    errors.append(message)


def _export_mermaid_flowchart(graph: Dict[str, object]) -> str:
    """Export graph into a Mermaid flowchart."""

    lines = ["flowchart LR"]
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])
    for node in nodes:
        node_id = _mermaid_id(node["id"])
        label = _mermaid_label(node)
        open_shape, close_shape = _node_shape(node.get("type", ""))
        lines.append(f"  {node_id}{open_shape}{label}{close_shape}")
    for edge in edges:
        from_id = _mermaid_id(edge["from"])
        to_id = _mermaid_id(edge["to"])
        edge_label = edge.get("type", "")
        lines.append(f"  {from_id} -->|{edge_label}| {to_id}")
    return "\n".join(lines)


def _export_mermaid_er(graph: Dict[str, object]) -> str:
    """Export graph into Mermaid ER diagram syntax."""

    lines = ["erDiagram"]
    tables = [node for node in graph.get("nodes", []) if node.get("type") == "table"]
    for table in tables:
        table_name = _sanitize_er_name(table.get("full_name", table.get("name", "")))
        columns = table.get("columns", [])
        lines.append(f"  {table_name} {{")
        for column in columns:
            column_name = _sanitize_er_name(column)
            lines.append(f"    string {column_name}")
        lines.append("  }")
    for edge in graph.get("edges", []):
        if edge.get("type") not in {"table_lineage", "joins_with"}:
            continue
        from_table = _sanitize_er_name(edge.get("from", ""))
        to_table = _sanitize_er_name(edge.get("to", ""))
        label = edge.get("type", "")
        lines.append(f"  {from_table} ||--o{{ {to_table} : {label}")
    return "\n".join(lines)


def _export_graphviz_dot(graph: Dict[str, object]) -> str:
    """Export graph into Graphviz DOT syntax."""

    lines = ["digraph lineage {"]
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])
    clusters: Dict[int, List[Dict[str, object]]] = {}
    for node in nodes:
        statement_index = node.get("statement_index", 0)
        clusters.setdefault(statement_index, []).append(node)
    for statement_index, cluster_nodes in clusters.items():
        lines.append(f'  subgraph "cluster_{statement_index}" {{')
        lines.append(f'    label="Statement {statement_index}"')
        for node in cluster_nodes:
            node_id = _dot_id(node["id"])
            label = _dot_label(node)
            lines.append(f'    {node_id} [label="{label}"];')
        lines.append("  }")
    for edge in edges:
        from_id = _dot_id(edge["from"])
        to_id = _dot_id(edge["to"])
        label = edge.get("type", "")
        lines.append(f'  {from_id} -> {to_id} [label="{label}"];')
    lines.append("}")
    return "\n".join(lines)


def _mermaid_id(node_id: str) -> str:
    """Convert node id to Mermaid-friendly identifier."""

    return "".join(char if char.isalnum() else "_" for char in node_id)


def _node_shape(node_type: str) -> tuple[str, str]:
    """Return Mermaid shape markers based on node type."""

    if node_type == "expression":
        return "{", "}"
    if node_type == "column":
        return "((", "))"
    return "[", "]"


def _mermaid_label(node: Dict[str, object]) -> str:
    """Build a Mermaid node label."""

    if node.get("type") == "column":
        label = node.get("name", "")
    elif node.get("type") == "expression":
        label = node.get("sql", "")
    else:
        label = node.get("full_name", node.get("name", ""))
    return f'"{label}"'


def _sanitize_er_name(name: str) -> str:
    """Sanitize names for Mermaid ER diagrams."""

    return "".join(char if char.isalnum() else "_" for char in name)


def _dot_id(node_id: str) -> str:
    """Sanitize node ids for DOT."""

    return f'"{node_id}"'


def _dot_label(node: Dict[str, object]) -> str:
    """Create a node label for DOT."""

    if node.get("type") == "column":
        return node.get("name", "")
    if node.get("type") == "expression":
        return node.get("sql", "")
    return node.get("full_name", node.get("name", ""))

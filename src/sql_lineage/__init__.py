"""Public interface for the sql_lineage package."""

from __future__ import annotations

from sql_lineage.analyzer import analyze, to_json
from sql_lineage.exporters import export_graph
from sql_lineage.graph import build_er_columns, build_graph

__all__ = ["analyze", "build_er_columns", "build_graph", "export_graph", "to_json"]

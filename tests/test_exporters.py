from __future__ import annotations

from pathlib import Path

from sql_lineage.exporters import export_graph
from sql_lineage.graph import build_graph


def _load_fixture(name: str) -> str:
    """Load SQL fixture content."""

    return Path(__file__).parent.joinpath("fixtures", name).read_text(encoding="utf-8")


def test_export_mermaid_flowchart_not_empty() -> None:
    sql = _load_fixture("mysql_complex.sql")
    graph = build_graph(sql, dialect="mysql", mode="full")
    output = export_graph(graph, format="mermaid_flowchart")
    assert "flowchart" in output


def test_export_graphviz_dot_not_empty() -> None:
    sql = _load_fixture("mysql_complex.sql")
    graph = build_graph(sql, dialect="mysql", mode="full")
    output = export_graph(graph, format="graphviz_dot")
    assert "digraph" in output

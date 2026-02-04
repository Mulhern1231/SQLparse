from __future__ import annotations

from pathlib import Path

from sql_lineage.graph import build_er_columns


def _load_fixture(name: str) -> str:
    """Load SQL fixture content."""

    return Path(__file__).parent.joinpath("fixtures", name).read_text(encoding="utf-8")


def test_er_columns_has_table_and_column_nodes() -> None:
    sql = _load_fixture("mysql_complex.sql")
    graph = build_er_columns(sql, dialect="mysql")

    tables = [node for node in graph["nodes"] if node["type"] == "table"]
    assert any(table["columns"] for table in tables)

    column_nodes = [node for node in graph["nodes"] if node["type"] == "column"]
    assert column_nodes

    coalesce_edges = [
        edge
        for edge in graph["edges"]
        if edge["type"] == "col_lineage"
        and edge["to"] == "column:reporting.user_orders.label"
    ]
    assert len(coalesce_edges) >= 2

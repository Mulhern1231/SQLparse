from __future__ import annotations

from pathlib import Path

from sql_lineage.graph import build_graph


def _load_fixture(name: str) -> str:
    """Load SQL fixture content."""

    return Path(__file__).parent.joinpath("fixtures", name).read_text(encoding="utf-8")


def test_graph_full_contains_all_tables_and_edges() -> None:
    sql = _load_fixture("postgres_complex.sql")
    graph = build_graph(sql, dialect="postgres", mode="full")

    table_ids = {node["id"] for node in graph["nodes"] if node["type"] == "table"}
    assert "table:core.users" in table_ids
    assert "table:core.orders" in table_ids
    assert "table:analytics.result_table" in table_ids

    lineage_edges = [
        edge
        for edge in graph["edges"]
        if edge["type"] == "lineage"
        and edge["from"] == "column:core.users.id"
        and edge["to"] == "column:analytics.result_table.user_id"
    ]
    assert lineage_edges
    assert all(node.get("description") for node in graph["nodes"])
    assert all(edge.get("description") for edge in graph["edges"])

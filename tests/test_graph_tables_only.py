from __future__ import annotations

from pathlib import Path

from sql_lineage.graph import build_graph


def _load_fixture(name: str) -> str:
    """Load SQL fixture content."""

    return Path(__file__).parent.joinpath("fixtures", name).read_text(encoding="utf-8")


def test_tables_only_has_aggregated_lineage() -> None:
    sql = _load_fixture("clickhouse_complex.sql")
    graph = build_graph(sql, dialect="clickhouse", mode="tables_only")

    edges = [
        edge
        for edge in graph["edges"]
        if edge["type"] == "table_lineage"
        and edge["from"] == "table:core.users"
        and edge["to"] == "table:analytics.ch_result"
    ]
    assert edges

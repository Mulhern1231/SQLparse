"""Command line interface for sql-lineage."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from sql_lineage.analyzer import to_json
from sql_lineage.exporters import export_graph
from sql_lineage.graph import build_graph


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SQL lineage analyzer")
    subparsers = parser.add_subparsers(dest="command")

    analyze_parser = subparsers.add_parser("analyze", help="Analyze SQL lineage")
    analyze_parser.add_argument("--sql", help="SQL string to analyze")
    analyze_parser.add_argument("--file", help="Path to SQL file")
    analyze_parser.add_argument("--dialect", default="clickhouse", help="SQL dialect")

    graph_parser = subparsers.add_parser("graph", help="Build lineage graph")
    graph_parser.add_argument("--sql", help="SQL string to analyze")
    graph_parser.add_argument("--file", help="Path to SQL file")
    graph_parser.add_argument("--dialect", default="clickhouse", help="SQL dialect")
    graph_parser.add_argument("--mode", default="full", help="Graph mode")
    graph_parser.add_argument(
        "--format",
        default="json",
        help="Export format: json, mermaid_flowchart, mermaid_er, graphviz_dot",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the SQL lineage CLI."""

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "analyze":
        sql = _read_sql(args.sql, args.file, parser)
        sys.stdout.write(to_json(sql, dialect=args.dialect))
        sys.stdout.write("\n")
        return 0
    if args.command == "graph":
        sql = _read_sql(args.sql, args.file, parser)
        graph = build_graph(sql, dialect=args.dialect, mode=args.mode)
        sys.stdout.write(export_graph(graph, format=args.format))
        sys.stdout.write("\n")
        return 0

    parser.print_help()
    return 2


def _read_file(path: str) -> str:
    """Read SQL from a file path."""

    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _read_sql(
    sql: str | None, file_path: str | None, parser: argparse.ArgumentParser
) -> str:
    """Resolve SQL from CLI arguments."""

    if file_path:
        return _read_file(file_path)
    if sql:
        return sql
    parser.error("Provide a SQL string or --file path")
    return ""


if __name__ == "__main__":
    raise SystemExit(main())

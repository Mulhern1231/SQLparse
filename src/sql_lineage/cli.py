"""Command line interface for sql-lineage."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from sql_lineage.analyzer import to_json


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SQL lineage analyzer")
    parser.add_argument("sql", nargs="?", help="SQL string to analyze")
    parser.add_argument("--file", help="Path to SQL file")
    parser.add_argument("--dialect", default="clickhouse", help="SQL dialect")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the SQL lineage CLI."""

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.file:
        sql = _read_file(args.file)
    elif args.sql:
        sql = args.sql
    else:
        parser.error("Provide a SQL string or --file path")
        return 2

    sys.stdout.write(to_json(sql, dialect=args.dialect))
    sys.stdout.write("\n")
    return 0


def _read_file(path: str) -> str:
    """Read SQL from a file path."""

    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


if __name__ == "__main__":
    raise SystemExit(main())

"""Parsing utilities for SQL lineage analysis."""

from __future__ import annotations

from typing import Optional

from sqlglot import exp, parse_one


class ParseResult:
    """Container for parsed SQL metadata."""

    def __init__(self, expression: exp.Expression, target: Optional[exp.Table]) -> None:
        self.expression = expression
        self.target = target


def parse_sql(sql: str, dialect: str) -> ParseResult:
    """Parse SQL into an AST and extract the target table if present."""

    expression = parse_one(sql, read=dialect)
    if isinstance(expression, exp.Create):
        target = expression.this if isinstance(expression.this, exp.Table) else None
        query = expression.expression
        if query is None:
            return ParseResult(expression, target)
        return ParseResult(query, target)
    return ParseResult(expression, None)

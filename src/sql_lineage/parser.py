"""Parsing utilities for SQL lineage analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from sqlglot import exp, parse


@dataclass(frozen=True)
class StatementParseResult:
    """Parsed SQL statement metadata."""

    expression: exp.Expression
    target: Optional[exp.Table]
    statement_type: str


def _statement_type(expression: exp.Expression) -> str:
    """Determine a statement type string for an expression."""

    if isinstance(expression, exp.Create):
        if expression.args.get("expression") is not None:
            return "create_table_as"
        return "create_table"
    if isinstance(expression, exp.Select):
        return "select"
    if isinstance(expression, exp.Union):
        return "union"
    return expression.key.lower()


def parse_sql(sql: str, dialect: str) -> List[StatementParseResult]:
    """Parse SQL into AST statements and extract metadata."""

    expressions = parse(sql, read=dialect)
    statements: List[StatementParseResult] = []
    for expression in expressions:
        target: Optional[exp.Table] = None
        if isinstance(expression, exp.Create):
            if isinstance(expression.this, exp.Table):
                target = expression.this
        statements.append(
            StatementParseResult(
                expression=expression,
                target=target,
                statement_type=_statement_type(expression),
            )
        )
    return statements

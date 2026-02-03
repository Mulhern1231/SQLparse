"""Collectors for SQL lineage components."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from sqlglot import exp


def table_identifier(table: exp.Table) -> Dict[str, str]:
    """Build a table identifier from a sqlglot Table expression."""

    name = table.name
    database = table.db or ""
    alias = table.alias_or_name
    return {
        "type": "table",
        "name": f"{database}.{name}".strip("."),
        "database": database,
        "alias": alias if alias != name else "",
    }


def collect_sources(expression: exp.Expression) -> List[Dict[str, str]]:
    """Collect table sources from an expression."""

    sources: List[Dict[str, str]] = []
    seen: set[Tuple[str, str, str]] = set()
    for table in expression.find_all(exp.Table):
        info = table_identifier(table)
        key = (info["name"], info.get("database", ""), info.get("alias", ""))
        if key in seen:
            continue
        seen.add(key)
        sources.append(info)
    return sources


def collect_joins(select: exp.Select, dialect: str) -> List[Dict[str, object]]:
    """Collect join metadata from a Select expression."""

    joins: List[Dict[str, object]] = []
    for join in select.args.get("joins", []) or []:
        right = join.this
        right_entry: Optional[Dict[str, str]] = None
        if isinstance(right, exp.Table):
            right_entry = table_identifier(right)
        join_type = (join.args.get("kind") or "inner").lower()
        condition = ""
        if join.args.get("on") is not None:
            condition = join.args["on"].sql(dialect=dialect)
        joins.append(
            {"join_type": join_type, "right": right_entry, "condition": condition}
        )
    return joins


def collect_subqueries(
    expression: exp.Expression, dialect: str
) -> List[Dict[str, object]]:
    """Collect subquery analyses from an expression."""

    from sql_lineage.analyzer import analyze_expression

    subqueries: List[Dict[str, object]] = []
    for subquery in expression.find_all(exp.Subquery):
        if isinstance(subquery.this, exp.Select):
            subqueries.append(analyze_expression(subquery.this, dialect))
    return subqueries

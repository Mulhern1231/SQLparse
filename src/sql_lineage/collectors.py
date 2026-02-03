"""Collectors for SQL lineage components."""

from __future__ import annotations

from typing import Dict, List, Optional

from sqlglot import exp


def collect_joins(select: exp.Select, dialect: str) -> List[Dict[str, object]]:
    """Collect join metadata from a Select expression."""

    joins: List[Dict[str, object]] = []
    for join in select.args.get("joins", []) or []:
        right = join.this
        right_entry: Optional[Dict[str, str]] = None
        if isinstance(right, exp.Table):
            name = right.name
            database = right.db or ""
            alias = right.alias_or_name
            right_entry = {
                "type": "table",
                "name": f"{database}.{name}".strip("."),
                "database": database,
                "alias": alias if alias != name else "",
            }
        elif isinstance(right, exp.Subquery):
            alias = right.alias_or_name
            right_entry = {
                "type": "subquery",
                "name": alias or "",
                "database": "",
                "alias": alias or "",
            }
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

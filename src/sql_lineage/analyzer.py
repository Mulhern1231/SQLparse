"""Top-level SQL lineage analyzer."""

from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple

from sqlglot import exp

from sql_lineage.collectors import collect_joins, collect_sources, collect_subqueries
from sql_lineage.dialects import is_supported_dialect, normalize_dialect
from sql_lineage.lineage import extract_lineage
from sql_lineage.parser import parse_sql


def _output_name(expression: exp.Expression) -> str:
    if isinstance(expression, exp.Alias):
        return expression.alias
    if isinstance(expression, exp.Column):
        return expression.name
    return expression.sql()


def _expression_sql(expression: exp.Expression, dialect: str) -> str:
    return expression.sql(dialect=dialect)


def analyze_select(select: exp.Select, dialect: str) -> Dict[str, object]:
    """Analyze a Select expression and return lineage metadata."""

    output_columns: List[Dict[str, object]] = []
    for select_expr in select.expressions:
        name = _output_name(select_expr)
        expression_sql = _expression_sql(select_expr, dialect)
        lineage = extract_lineage(select_expr)
        output_columns.append(
            {"name": name, "expression": expression_sql, "lineage": lineage}
        )

    return {
        "sources": collect_sources(select),
        "output": {"columns": output_columns},
        "joins": collect_joins(select, dialect),
        "unions": [],
        "subqueries": collect_subqueries(select, dialect),
    }


def analyze_union(union: exp.Union, dialect: str) -> Dict[str, object]:
    """Analyze a Union expression and return lineage metadata."""

    left = union.left
    right = union.right
    left_data = analyze_expression(left, dialect)
    right_data = analyze_expression(right, dialect)

    output_columns: List[Dict[str, object]] = []
    left_columns = left_data.get("output", {}).get("columns", [])
    right_columns = right_data.get("output", {}).get("columns", [])

    for index, left_col in enumerate(left_columns):
        right_col = right_columns[index] if index < len(right_columns) else None
        inputs = list(left_col["lineage"]["inputs"])
        if right_col:
            inputs.extend(right_col["lineage"]["inputs"])
        lineage = {
            "type": "union",
            "inputs": inputs,
            "functions": [],
            "literals": [],
            "notes": [],
        }
        output_columns.append(
            {
                "name": left_col["name"],
                "expression": left_col["expression"],
                "lineage": lineage,
            }
        )

    unions = [left_data, right_data]
    sources = left_data.get("sources", []) + right_data.get("sources", [])
    joins = left_data.get("joins", []) + right_data.get("joins", [])
    subqueries = left_data.get("subqueries", []) + right_data.get("subqueries", [])

    return {
        "sources": sources,
        "output": {"columns": output_columns},
        "joins": joins,
        "unions": unions,
        "subqueries": subqueries,
    }


def analyze_expression(expression: exp.Expression, dialect: str) -> Dict[str, object]:
    """Analyze a generic SQL expression (Select or Union)."""

    if isinstance(expression, exp.Select):
        return analyze_select(expression, dialect)
    if isinstance(expression, exp.Union):
        return analyze_union(expression, dialect)
    select = expression.find(exp.Select)
    if select is not None:
        return analyze_select(select, dialect)
    return {
        "sources": [],
        "output": {"columns": []},
        "joins": [],
        "unions": [],
        "subqueries": [],
    }


def analyze(sql: str, dialect: str = "clickhouse") -> Dict[str, object]:
    """Analyze SQL and return a JSON-compatible lineage dictionary."""

    normalized_dialect = normalize_dialect(dialect)
    errors: List[str] = []
    target: Optional[Dict[str, str]] = None
    if not is_supported_dialect(normalized_dialect):
        errors.append(f"Unsupported dialect: {dialect}")
    try:
        parse_result = parse_sql(sql, normalized_dialect)
    except Exception as exc:  # pragma: no cover - defensive for parser errors
        return {
            "dialect": normalized_dialect,
            "target": None,
            "sources": [],
            "output": {"columns": []},
            "joins": [],
            "unions": [],
            "subqueries": [],
            "errors": [str(exc)],
        }

    if parse_result.target is not None:
        target = {
            "type": "table",
            "name": parse_result.target.name,
            "database": parse_result.target.db or "",
            "raw": parse_result.target.sql(dialect=normalized_dialect),
        }

    analysis = analyze_expression(parse_result.expression, normalized_dialect)
    analysis["dialect"] = normalized_dialect
    analysis["target"] = target
    analysis["errors"] = errors
    return analysis


def to_json(sql: str, dialect: str = "clickhouse", indent: int = 2) -> str:
    """Serialize lineage analysis into JSON."""

    return json.dumps(analyze(sql, dialect=dialect), indent=indent, ensure_ascii=False)

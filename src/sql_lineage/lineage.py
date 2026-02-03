"""Expression lineage extraction utilities."""

from __future__ import annotations

from typing import Dict, List, Set, Tuple

from sqlglot import exp


def _unique_inputs(inputs: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: Set[Tuple[str, str]] = set()
    unique: List[Dict[str, str]] = []
    for item in inputs:
        key = (item.get("table", ""), item.get("column", ""))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _function_name(func: exp.Expression) -> str:
    if hasattr(func, "sql_name"):
        return func.sql_name().lower()
    if hasattr(func, "name"):
        return str(func.name).lower()
    return func.__class__.__name__.lower()


def extract_lineage(expression: exp.Expression) -> Dict[str, object]:
    """Extract column inputs, functions, and literals from an expression."""

    inputs = [
        {"table": col.table or "", "column": col.name}
        for col in expression.find_all(exp.Column)
    ]
    functions = [
        _function_name(func) for func in expression.find_all((exp.Func, exp.Anonymous))
    ]
    literals = [literal.this for literal in expression.find_all(exp.Literal)]
    return {
        "type": "expression",
        "inputs": _unique_inputs(inputs),
        "functions": sorted(set(functions)),
        "literals": literals,
        "notes": [],
    }

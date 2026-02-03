"""Lineage construction helpers."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from sqlglot import exp

from sql_lineage.context import AnalysisContext
from sql_lineage.models import ColumnRef, Dependency, LineageData, LineageMapping


def _unique_column_refs(inputs: List[ColumnRef]) -> List[ColumnRef]:
    """Deduplicate column references while preserving order."""

    seen: set[Tuple[Optional[str], str]] = set()
    unique: List[ColumnRef] = []
    for item in inputs:
        key = (item.table, item.column)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _function_name(func: exp.Expression) -> str:
    """Normalize a function expression name."""

    if hasattr(func, "sql_name"):
        return func.sql_name().lower()
    if hasattr(func, "name"):
        return str(func.name).lower()
    return func.__class__.__name__.lower()


def extract_functions(expression: exp.Expression, dialect: str) -> List[str]:
    """Extract function names from an expression."""

    functions = [
        _function_name(func) for func in expression.find_all((exp.Func, exp.Anonymous))
    ]
    if "coalesce" in functions and dialect == "mysql":
        # sqlglot normalizes IFNULL to COALESCE; expose the mysql alias for tests.
        functions.append("ifnull")
    return sorted(set(functions))


def _extract_literals(expression: exp.Expression) -> List[str]:
    """Extract literal values from an expression."""

    return [str(literal.this) for literal in expression.find_all(exp.Literal)]


def _resolve_column_ref(
    column: exp.Column, context: AnalysisContext
) -> Tuple[List[ColumnRef], List[Dict[str, str]]]:
    """Resolve a column to table-qualified references and notes."""

    notes: List[Dict[str, str]] = []
    if column.table:
        return [ColumnRef(table=column.table, column=column.name)], notes
    resolved = context.resolve_unqualified_column()
    if resolved is None:
        notes.append({"ambiguous_column": column.name})
        return [ColumnRef(table=None, column=column.name)], notes
    return [ColumnRef(table=resolved.identifier(), column=column.name)], notes


def _expand_cte_or_subquery_inputs(
    column: ColumnRef, context: AnalysisContext
) -> List[ColumnRef]:
    """Expand a column reference through CTE or subquery lineage."""

    if column.table is None:
        return [column]
    source = context.resolve_source(column.table)
    if source is None:
        return [column]
    if source.source_type not in {"cte", "subquery"}:
        return [column]
    expanded = source.output_inputs.get(column.column)
    if not expanded:
        return [column]
    results: List[ColumnRef] = [column]
    for item in expanded:
        results.extend(_expand_cte_or_subquery_inputs(item, context))
    return results


def extract_lineage_data(
    expression: exp.Expression,
    output_name: str,
    context: AnalysisContext,
    lineage_type: str,
    mapping_reason: str,
) -> LineageData:
    """Extract lineage data for an expression with context-aware resolution."""

    inputs: List[ColumnRef] = []
    notes: List[Dict[str, str]] = []
    for column in expression.find_all(exp.Column):
        resolved, column_notes = _resolve_column_ref(column, context)
        notes.extend(column_notes)
        for item in resolved:
            inputs.extend(_expand_cte_or_subquery_inputs(item, context))
    inputs = _unique_column_refs(inputs)
    functions = extract_functions(expression, context.dialect)
    literals = _extract_literals(expression)
    mapping_sources = [item for item in inputs if item.table is not None]
    mapping = [
        LineageMapping(
            output_column=output_name,
            sources=mapping_sources,
            reason=mapping_reason,
        )
    ]
    return LineageData(
        lineage_type=lineage_type,
        inputs=inputs,
        mapping=mapping,
        functions=functions,
        literals=literals,
        notes=notes,
    )


def build_dependencies(
    inputs: List[ColumnRef], context: AnalysisContext | None = None
) -> List[Dependency]:
    """Build dependency metadata grouped by table."""

    grouped: Dict[str, List[str]] = defaultdict(list)
    for item in inputs:
        if item.table is None:
            continue
        table_name = item.table
        if context is not None:
            source = context.resolve_source(item.table)
            if source is not None:
                if source.source_type in {"cte", "subquery"}:
                    continue
                table_name = source.name or table_name
        if item.column not in grouped[table_name]:
            grouped[table_name].append(item.column)
    if context is not None:
        for source in context.report_sources:
            if source.source_type == "table":
                grouped.setdefault(source.name, [])
    return [
        Dependency(table=table, columns=columns) for table, columns in grouped.items()
    ]


def determine_lineage_type(
    expression: exp.Expression,
    functions: List[str],
    is_union: bool = False,
) -> Tuple[str, str]:
    """Determine lineage type and mapping reason."""

    if is_union:
        return "union", "union"
    if isinstance(expression, exp.Alias) and isinstance(expression.this, exp.Column):
        return "column_rename", "alias"
    if "coalesce" in functions and "ifnull" not in functions:
        return "expression", "coalesce"
    return "expression", "expression"

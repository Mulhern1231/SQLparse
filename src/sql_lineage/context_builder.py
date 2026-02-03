"""Context builder for SQL lineage analysis."""

from __future__ import annotations

from typing import Dict, List, Tuple

from sqlglot import exp

from sql_lineage.context import (
    AnalysisContext,
    SourceInfo,
    build_source_info_from_cte,
    build_source_info_from_subquery,
    build_source_info_from_table,
    merge_sources,
)
from sql_lineage.models import ColumnRef


def _output_inputs_from_analysis(
    analysis: Dict[str, object],
) -> Dict[str, List[ColumnRef]]:
    """Extract output-input mappings from a lineage analysis dictionary."""

    output_inputs: Dict[str, List[ColumnRef]] = {}
    output = analysis.get("output", {})
    for column in output.get("columns", []):
        name = column["name"]
        lineage_inputs = column["lineage"]["inputs"]
        output_inputs[name] = [
            ColumnRef(table=item.get("table"), column=item.get("column"))
            for item in lineage_inputs
        ]
    return output_inputs


def _collect_cte_sources(
    select: exp.Select, dialect: str, analyze_expression
) -> Tuple[List[SourceInfo], List[SourceInfo]]:
    """Collect CTE sources for resolution and reporting."""

    sources: List[SourceInfo] = []
    report_sources: List[SourceInfo] = []
    cte_sources: Dict[str, SourceInfo] = {}
    with_clause = select.args.get("with")
    if not isinstance(with_clause, exp.With):
        return sources, report_sources
    for cte in with_clause.expressions:
        if not isinstance(cte, exp.CTE):
            continue
        alias = cte.alias_or_name
        analysis = analyze_expression(cte.this, dialect)
        output_inputs = _output_inputs_from_analysis(analysis)
        cte_source = build_source_info_from_cte(alias, output_inputs)
        sources.append(cte_source)
        report_sources.append(cte_source)
        cte_sources[alias] = cte_source
        for source in analysis.get("sources", []):
            if source.get("type") == "table":
                report_sources.append(
                    SourceInfo(
                        name=source.get("name", ""),
                        alias=source.get("alias", ""),
                        database=source.get("database", ""),
                        source_type="table",
                    )
                )
    return sources, report_sources


def _collect_subquery_sources(
    select: exp.Select, dialect: str, analyze_expression
) -> Tuple[List[SourceInfo], List[SourceInfo]]:
    """Collect subquery sources for resolution and reporting."""

    sources: List[SourceInfo] = []
    report_sources: List[SourceInfo] = []
    for subquery in select.find_all(exp.Subquery):
        alias = subquery.alias_or_name
        if not alias:
            continue
        analysis = analyze_expression(subquery.this, dialect)
        output_inputs = _output_inputs_from_analysis(analysis)
        subquery_source = build_source_info_from_subquery(alias, output_inputs)
        sources.append(subquery_source)
        report_sources.append(subquery_source)
        for source in analysis.get("sources", []):
            if source.get("type") == "table":
                report_sources.append(
                    SourceInfo(
                        name=source.get("name", ""),
                        alias=source.get("alias", ""),
                        database=source.get("database", ""),
                        source_type="table",
                    )
                )
    return sources, report_sources


def _collect_immediate_tables(select: exp.Select) -> List[SourceInfo]:
    """Collect tables referenced directly in FROM/JOIN clauses."""

    sources: List[SourceInfo] = []
    from_clause = select.args.get("from")
    if isinstance(from_clause, exp.From):
        for expression in from_clause.expressions:
            if isinstance(expression, exp.Table):
                sources.append(build_source_info_from_table(expression))
    for join in select.args.get("joins", []) or []:
        if isinstance(join.this, exp.Table):
            sources.append(build_source_info_from_table(join.this))
    return sources


def build_context(
    select: exp.Select, dialect: str, analyze_expression
) -> AnalysisContext:
    """Build an analysis context for a Select expression."""

    sources: List[SourceInfo] = []
    report_sources: List[SourceInfo] = []
    cte_sources, cte_reports = _collect_cte_sources(select, dialect, analyze_expression)
    sources.extend(cte_sources)
    report_sources.extend(cte_reports)
    subquery_sources, subquery_reports = _collect_subquery_sources(
        select, dialect, analyze_expression
    )
    sources.extend(subquery_sources)
    report_sources.extend(subquery_reports)
    immediate_tables = _collect_immediate_tables(select)
    sources.extend(immediate_tables)
    report_sources.extend(immediate_tables)
    return AnalysisContext(
        sources=merge_sources(sources),
        report_sources=merge_sources(report_sources),
    )

"""Top-level SQL lineage analyzer."""

from __future__ import annotations

import json
from typing import List, Optional

from sqlglot import exp

from sql_lineage.collectors import collect_joins, collect_subqueries
from sql_lineage.context_builder import build_context
from sql_lineage.dialects import is_supported_dialect, normalize_dialect
from sql_lineage.lineage_builder import (
    build_dependencies,
    determine_lineage_type,
    extract_functions,
    extract_lineage_data,
)
from sql_lineage.models import (
    ColumnRef,
    Dependency,
    LineageData,
    LineageMapping,
    OutputColumn,
    StatementAnalysis,
)
from sql_lineage.parser import StatementParseResult, parse_sql


def _output_name(expression: exp.Expression) -> str:
    """Resolve the output name for a select expression."""

    if isinstance(expression, exp.Alias):
        return expression.alias
    if isinstance(expression, exp.Column):
        return expression.name
    return expression.sql()


def _expression_sql(expression: exp.Expression, dialect: str) -> str:
    """Render an expression SQL string without alias wrappers."""

    if isinstance(expression, exp.Alias):
        return expression.this.sql(dialect=dialect)
    return expression.sql(dialect=dialect)


def _analyze_select(select: exp.Select, dialect: str) -> Dict[str, object]:
    """Analyze a Select expression and return lineage metadata."""

    context = build_context(select, dialect, analyze_expression)
    output_columns: List[OutputColumn] = []
    for select_expr in select.expressions:
        name = _output_name(select_expr)
        expression_sql = _expression_sql(select_expr, dialect)
        lineage_expression = (
            select_expr.this if isinstance(select_expr, exp.Alias) else select_expr
        )
        functions = extract_functions(lineage_expression, dialect)
        lineage_type, mapping_reason = determine_lineage_type(select_expr, functions)
        lineage = extract_lineage_data(
            lineage_expression, name, context, lineage_type, mapping_reason
        )
        dependencies = build_dependencies(lineage.inputs, context)
        output_columns.append(
            OutputColumn(
                name=name,
                expression=expression_sql,
                lineage=lineage,
                dependencies=dependencies,
            )
        )

    sources = [
        {
            "type": source.source_type,
            "name": source.name,
            "database": source.database,
            "alias": source.alias,
        }
        for source in context.report_sources
    ]

    return {
        "sources": sources,
        "output": {"columns": [col.to_dict() for col in output_columns]},
        "joins": collect_joins(select, dialect),
        "unions": [],
        "subqueries": collect_subqueries(select, dialect),
    }


def _analyze_union(union: exp.Union, dialect: str) -> Dict[str, object]:
    """Analyze a Union expression and return lineage metadata."""

    left = union.left
    right = union.right
    left_data = analyze_expression(left, dialect)
    right_data = analyze_expression(right, dialect)

    output_columns: List[OutputColumn] = []
    left_columns = left_data.get("output", {}).get("columns", [])
    right_columns = right_data.get("output", {}).get("columns", [])

    for index, left_col in enumerate(left_columns):
        right_col = right_columns[index] if index < len(right_columns) else None
        inputs = list(left_col["lineage"]["inputs"])
        if right_col:
            inputs.extend(right_col["lineage"]["inputs"])
        lineage_inputs = [
            {
                "table": item.get("table"),
                "column": item.get("column"),
            }
            for item in inputs
        ]
        mapping_sources = [item for item in lineage_inputs if item.get("table")]
        dependencies = build_dependencies(
            [
                ColumnRef(table=item.get("table"), column=item.get("column"))
                for item in lineage_inputs
            ]
        )
        output_columns.append(
            OutputColumn(
                name=left_col["name"],
                expression=left_col["expression"],
                lineage=LineageData(
                    lineage_type="union",
                    inputs=[
                        ColumnRef(table=item.get("table"), column=item.get("column"))
                        for item in lineage_inputs
                    ],
                    mapping=[
                        LineageMapping(
                            output_column=left_col["name"],
                            sources=[
                                ColumnRef(
                                    table=item.get("table"),
                                    column=item.get("column"),
                                )
                                for item in mapping_sources
                            ],
                            reason="union",
                        )
                    ],
                    functions=[],
                    literals=[],
                    notes=[],
                ),
                dependencies=dependencies,
            )
        )

    unions = [left_data, right_data]
    sources = left_data.get("sources", []) + right_data.get("sources", [])
    joins = left_data.get("joins", []) + right_data.get("joins", [])
    subqueries = left_data.get("subqueries", []) + right_data.get("subqueries", [])

    return {
        "sources": sources,
        "output": {"columns": [col.to_dict() for col in output_columns]},
        "joins": joins,
        "unions": unions,
        "subqueries": subqueries,
    }


def analyze_expression(expression: exp.Expression, dialect: str) -> Dict[str, object]:
    """Analyze a generic SQL expression (Select or Union)."""

    if isinstance(expression, exp.Select):
        return _analyze_select(expression, dialect)
    if isinstance(expression, exp.Union):
        return _analyze_union(expression, dialect)
    select = expression.find(exp.Select)
    if select is not None:
        return _analyze_select(select, dialect)
    return {
        "sources": [],
        "output": {"columns": []},
        "joins": [],
        "unions": [],
        "subqueries": [],
    }


def _target_from_table(table: exp.Table, dialect: str) -> Dict[str, str]:
    """Create a target table dictionary from a sqlglot Table expression."""

    return {
        "type": "table",
        "name": table.name,
        "database": table.db or "",
        "raw": table.sql(dialect=dialect),
    }


def _analyze_statement(
    statement: StatementParseResult, dialect: str
) -> StatementAnalysis:
    """Analyze a parsed SQL statement and return a StatementAnalysis."""

    errors: List[str] = []
    expression = statement.expression
    analysis_expression = expression
    if (
        isinstance(expression, exp.Create)
        and expression.args.get("expression") is not None
    ):
        analysis_expression = expression.args["expression"]
    analysis = analyze_expression(analysis_expression, dialect)
    target: Optional[Dict[str, str]] = None
    if statement.target is not None:
        target = _target_from_table(statement.target, dialect)
    output_columns = [
        OutputColumn(
            name=col["name"],
            expression=col["expression"],
            lineage=LineageData(
                lineage_type=col["lineage"]["type"],
                inputs=[
                    ColumnRef(
                        table=item.get("table"),
                        column=item.get("column"),
                    )
                    for item in col["lineage"]["inputs"]
                ],
                mapping=[
                    LineageMapping(
                        output_column=mapping["output_column"],
                        sources=[
                            ColumnRef(
                                table=source.get("table"),
                                column=source.get("column"),
                            )
                            for source in mapping["sources"]
                        ],
                        reason=mapping["reason"],
                    )
                    for mapping in col["lineage"].get("mapping", [])
                ],
                functions=col["lineage"]["functions"],
                literals=col["lineage"]["literals"],
                notes=col["lineage"]["notes"],
            ),
            dependencies=[
                Dependency(
                    table=dep["table"],
                    columns=list(dep["columns"]),
                )
                for dep in col.get("dependencies", [])
            ],
        )
        for col in analysis.get("output", {}).get("columns", [])
    ]
    return StatementAnalysis(
        index=0,
        statement_type=statement.statement_type,
        target=target,
        output_columns=output_columns,
        sources=analysis.get("sources", []),
        joins=analysis.get("joins", []),
        unions=analysis.get("unions", []),
        subqueries=analysis.get("subqueries", []),
        errors=errors,
    )


def analyze(sql: str, dialect: str = "clickhouse") -> Dict[str, object]:
    """Analyze SQL and return a JSON-compatible lineage dictionary."""

    normalized_dialect = normalize_dialect(dialect)
    errors: List[str] = []
    if not is_supported_dialect(normalized_dialect):
        errors.append(f"Unsupported dialect: {dialect}")

    try:
        statements = parse_sql(sql, normalized_dialect)
        dialect_used = normalized_dialect
    except Exception as exc:
        try:
            statements = parse_sql(sql, "ansi")
            dialect_used = "ansi"
            errors.append(
                f"Failed to parse with dialect '{normalized_dialect}', using ansi: {exc}"
            )
        except Exception as fallback_exc:
            return {
                "dialect": normalized_dialect,
                "statements": [],
                "errors": errors + [str(fallback_exc)],
            }

    analyses: List[StatementAnalysis] = []
    for index, statement in enumerate(statements, start=1):
        analysis = _analyze_statement(statement, dialect_used)
        analyses.append(
            StatementAnalysis(
                index=index,
                statement_type=analysis.statement_type,
                target=analysis.target,
                output_columns=analysis.output_columns,
                sources=analysis.sources,
                joins=analysis.joins,
                unions=analysis.unions,
                subqueries=analysis.subqueries,
                errors=analysis.errors,
            )
        )

    return {
        "dialect": dialect_used,
        "statements": [analysis.to_dict() for analysis in analyses],
        "errors": errors,
    }


def to_json(sql: str, dialect: str = "clickhouse", indent: int = 2) -> str:
    """Serialize lineage analysis into JSON."""

    return json.dumps(analyze(sql, dialect=dialect), indent=indent, ensure_ascii=False)

"""Graph builder for SQL lineage."""

from __future__ import annotations

import datetime as dt
from typing import Dict, Iterable, List, Optional, Tuple

from sql_lineage.analyzer import analyze
from sql_lineage.graph_utils import (
    ResolvedTable,
    column_id,
    cte_id,
    ensure_unique_columns,
    expression_id,
    resolve_table_reference,
    split_table_name,
    subquery_id,
    table_id,
)


def build_graph(
    sql: str, dialect: str = "clickhouse", mode: str = "full"
) -> Dict[str, object]:
    """Build a lineage graph from SQL."""

    normalized_mode = mode.lower()
    analysis = analyze(sql, dialect=dialect)
    graph: Dict[str, object] = {
        "dialect": analysis["dialect"],
        "mode": normalized_mode,
        "meta": {
            "statements": len(analysis.get("statements", [])),
            "generated_at": dt.datetime.utcnow().replace(microsecond=0).isoformat()
            + "Z",
            "library": "sql_lineage",
            "version": "0.2.0",
        },
        "nodes": [],
        "edges": [],
        "errors": list(analysis.get("errors", [])),
        "warnings": [],
    }
    if normalized_mode not in {"full", "er_columns", "tables_only"}:
        graph["errors"].append(f"Unsupported graph mode: {mode}")
        normalized_mode = "full"
        graph["mode"] = normalized_mode

    builder = _GraphBuilder(graph)
    statements = analysis.get("statements", [])
    if normalized_mode == "tables_only":
        _build_tables_only_graph(builder, statements)
    elif normalized_mode == "er_columns":
        _build_er_columns_graph(builder, statements)
    else:
        _build_full_graph(builder, statements)
    return builder.finalize()


def build_er_columns(sql: str, dialect: str = "clickhouse") -> Dict[str, object]:
    """Convenience wrapper for ER column graphs."""

    return build_graph(sql, dialect=dialect, mode="er_columns")


class _GraphBuilder:
    """Helper for assembling graph nodes and edges."""

    def __init__(self, graph: Dict[str, object]) -> None:
        self.graph = graph
        self.nodes: Dict[str, Dict[str, object]] = {}
        self.edges: List[Dict[str, object]] = []
        self.edge_count = 0

    def add_node(self, node: Dict[str, object]) -> None:
        """Add a node if not already present."""

        node_id = node["id"]
        if node_id in self.nodes:
            return
        self.nodes[node_id] = node

    def add_edge(
        self,
        edge_type: str,
        from_node: str,
        to_node: str,
        description: str,
        statement_index: int,
        details: Optional[Dict[str, object]] = None,
    ) -> None:
        """Add an edge entry."""

        self.edge_count += 1
        edge = {
            "id": f"edge:{edge_type}:{self.edge_count}",
            "type": edge_type,
            "from": from_node,
            "to": to_node,
            "description": description,
            "statement_index": statement_index,
            "details": details or {},
        }
        self.edges.append(edge)

    def add_warning(
        self, code: str, message: str, statement_index: int, context: str
    ) -> None:
        """Record a warning entry."""

        warnings: List[Dict[str, object]] = self.graph["warnings"]
        warnings.append(
            {
                "code": code,
                "message": message,
                "statement_index": statement_index,
                "context": context,
            }
        )

    def finalize(self) -> Dict[str, object]:
        """Finalize the graph for output."""

        self.graph["nodes"] = list(self.nodes.values())
        self.graph["edges"] = self.edges
        return self.graph


def _build_full_graph(
    builder: _GraphBuilder, statements: Iterable[Dict[str, object]]
) -> None:
    """Build full dependency graph."""

    for statement in statements:
        statement_index = statement["index"]
        sources = statement.get("sources", [])
        subquery_map = _build_subquery_map(statement_index, sources)
        target_table = _target_table_from_statement(statement)
        _add_source_nodes(builder, sources, statement_index, subquery_map)
        if target_table:
            _add_table_node(
                builder, target_table, statement_index, "table", "Target table"
            )
        subquery_index = 0
        for _subquery in statement.get("subqueries", []) or []:
            subquery_index += 1
            node_id = subquery_id(statement_index, subquery_index)
            builder.add_node(
                {
                    "id": node_id,
                    "type": "subquery",
                    "name": f"subquery_{statement_index}_{subquery_index}",
                    "statement_index": statement_index,
                    "description": "Subquery in statement",
                }
            )

        output_columns = statement.get("output", {}).get("columns", [])
        for output_column in output_columns:
            _add_output_column_graph(
                builder,
                output_column,
                statement_index,
                target_table,
                sources,
                subquery_map,
            )

        _add_join_edges(builder, sources, statement, statement_index, subquery_map)
        _add_union_edges(builder, sources, statement, statement_index, target_table)


def _build_er_columns_graph(
    builder: _GraphBuilder, statements: Iterable[Dict[str, object]]
) -> None:
    """Build ER graph with tables and columns."""

    for statement in statements:
        statement_index = statement["index"]
        sources = statement.get("sources", [])
        subquery_map = _build_subquery_map(statement_index, sources)
        target_table = _target_table_from_statement(statement)
        _add_source_nodes(builder, sources, statement_index, subquery_map)
        if target_table:
            _add_table_node(
                builder, target_table, statement_index, "table", "Target table"
            )

        output_columns = statement.get("output", {}).get("columns", [])
        for output_column in output_columns:
            _add_er_column_nodes(
                builder,
                output_column,
                statement_index,
                target_table,
                sources,
                subquery_map,
            )

        _add_er_column_edges(
            builder,
            output_columns,
            statement_index,
            sources,
            target_table,
            subquery_map,
        )
        _add_fk_like_edges(builder, statement, statement_index, sources, subquery_map)

    _ensure_table_columns(builder)


def _build_tables_only_graph(
    builder: _GraphBuilder, statements: Iterable[Dict[str, object]]
) -> None:
    """Build table-level lineage graph."""

    for statement in statements:
        statement_index = statement["index"]
        sources = statement.get("sources", [])
        subquery_map = _build_subquery_map(statement_index, sources)
        target_table = _target_table_from_statement(statement)
        _add_source_nodes(builder, sources, statement_index, subquery_map)
        if target_table:
            _add_table_node(
                builder, target_table, statement_index, "table", "Target table"
            )

        if not target_table:
            continue
        dependency_map: Dict[str, Dict[str, object]] = {}
        output_columns = statement.get("output", {}).get("columns", [])
        for output_column in output_columns:
            lineage = output_column.get("lineage", {})
            inputs = lineage.get("inputs", [])
            for input_ref in inputs:
                resolved, warning = _resolve_with_subqueries(
                    input_ref.get("table"), sources, statement_index, subquery_map
                )
                if warning:
                    builder.add_warning(
                        code="unresolved_table",
                        message=warning,
                        statement_index=statement_index,
                        context=str(input_ref),
                    )
                if resolved.full_name not in dependency_map:
                    dependency_map[resolved.full_name] = {
                        "count": 0,
                        "reasons": set(),
                    }
                dependency_map[resolved.full_name]["count"] += 1
                dependency_map[resolved.full_name]["reasons"].add(
                    lineage.get("type", "select")
                )
        for source_name, data in dependency_map.items():
            from_id = _table_node_id_from_source_name(
                source_name, sources, statement_index, subquery_map
            )
            to_id = table_id(target_table["full_name"])
            details = {
                "columns_count": data["count"],
                "via": sorted(data["reasons"]),
            }
            builder.add_edge(
                "table_lineage",
                from_id,
                to_id,
                "Table-level lineage",
                statement_index,
                details,
            )


def _target_table_from_statement(
    statement: Dict[str, object],
) -> Optional[Dict[str, str]]:
    """Extract target table metadata from a statement."""

    target = statement.get("target")
    if not target:
        return None
    database = target.get("database", "")
    name = target.get("name", "")
    full_name = name if not database else f"{database}.{name}"
    return {
        "full_name": full_name,
        "database": database,
        "name": name,
        "schema": database or "",
    }


def _add_source_nodes(
    builder: _GraphBuilder,
    sources: Iterable[Dict[str, str]],
    statement_index: int,
    subquery_map: Dict[str, str],
) -> None:
    """Add nodes for source tables/CTEs/subqueries."""

    for source in sources:
        source_type = source.get("type", "table")
        name = source.get("name", "")
        if source_type == "table":
            database, table_name = split_table_name(name)
            full_name = name
            builder.add_node(
                {
                    "id": table_id(full_name),
                    "type": "table",
                    "name": table_name,
                    "database": database,
                    "schema": database or "",
                    "full_name": full_name,
                    "statement_index": statement_index,
                    "description": "Source table",
                }
            )
        elif source_type == "cte":
            builder.add_node(
                {
                    "id": cte_id(name),
                    "type": "cte",
                    "name": name,
                    "statement_index": statement_index,
                    "description": "Common table expression",
                }
            )
        elif source_type == "subquery":
            subquery_node_id = subquery_map.get(
                name, f"subquery:{statement_index}:{name}"
            )
            builder.add_node(
                {
                    "id": subquery_node_id,
                    "type": "subquery",
                    "name": name,
                    "statement_index": statement_index,
                    "description": "Subquery source",
                }
            )
        else:
            builder.add_node(
                {
                    "id": table_id(name or "unknown"),
                    "type": "table",
                    "name": name or "unknown",
                    "database": "",
                    "schema": "",
                    "full_name": name or "unknown",
                    "statement_index": statement_index,
                    "description": "Unknown source",
                }
            )


def _add_table_node(
    builder: _GraphBuilder,
    table: Dict[str, str],
    statement_index: int,
    table_type: str,
    description: str,
) -> None:
    """Add a table-like node."""

    if table_type == "cte":
        node_id = cte_id(table["name"])
    elif table_type == "subquery":
        node_id = f"subquery:{statement_index}:{table['name']}"
    else:
        node_id = table_id(table["full_name"])
    builder.add_node(
        {
            "id": node_id,
            "type": table_type,
            "name": table.get("name", ""),
            "database": table.get("database", ""),
            "schema": table.get("schema", ""),
            "full_name": table.get("full_name", ""),
            "statement_index": statement_index,
            "description": description,
        }
    )


def _add_output_column_graph(
    builder: _GraphBuilder,
    output_column: Dict[str, object],
    statement_index: int,
    target_table: Optional[Dict[str, str]],
    sources: Iterable[Dict[str, str]],
    subquery_map: Dict[str, str],
) -> None:
    """Add nodes and edges for output column lineage."""

    lineage = output_column.get("lineage", {})
    output_name = output_column.get("name", "")
    target_full = target_table["full_name"] if target_table else "unknown"
    if target_full == "unknown":
        builder.add_node(
            {
                "id": table_id("unknown"),
                "type": "table",
                "name": "unknown",
                "database": "",
                "schema": "",
                "full_name": "unknown",
                "statement_index": statement_index,
                "description": "Unknown target table",
            }
        )
    output_col_id = column_id(target_full, output_name)
    builder.add_node(
        {
            "id": output_col_id,
            "type": "column",
            "table_id": table_id(target_full),
            "name": output_name,
            "data_type": None,
            "description": "Output column",
            "literals": lineage.get("literals", []),
            "statement_index": statement_index,
        }
    )
    builder.add_edge(
        "contains",
        table_id(target_full),
        output_col_id,
        "Table contains column",
        statement_index,
        {},
    )

    expression_node_id = None
    if _requires_expression_node(output_column):
        expression_sql = output_column.get("expression", "")
        expression_node_id = expression_id(statement_index, output_name, expression_sql)
        builder.add_node(
            {
                "id": expression_node_id,
                "type": "expression",
                "sql": expression_sql,
                "description": "Expression producing output column",
                "statement_index": statement_index,
            }
        )
        builder.add_edge(
            "produces",
            expression_node_id,
            output_col_id,
            "Expression produces output column",
            statement_index,
            {"function": lineage.get("type", "")},
        )

    for input_ref in lineage.get("inputs", []):
        resolved, warning = _resolve_with_subqueries(
            input_ref.get("table"), sources, statement_index, subquery_map
        )
        if warning:
            builder.add_warning(
                code="unresolved_table",
                message=warning,
                statement_index=statement_index,
                context=str(input_ref),
            )
        input_table_name = _resolved_full_name(resolved)
        input_col_id = column_id(input_table_name, input_ref.get("column", "unknown"))
        builder.add_node(
            {
                "id": input_col_id,
                "type": "column",
                "table_id": _table_node_id_from_resolved(resolved),
                "name": input_ref.get("column", "unknown"),
                "data_type": None,
                "description": "Input column",
                "statement_index": statement_index,
            }
        )
        builder.add_edge(
            "contains",
            _table_node_id_from_resolved(resolved),
            input_col_id,
            "Table contains column",
            statement_index,
            {},
        )
        builder.add_edge(
            "lineage",
            input_col_id,
            output_col_id,
            "Column-level lineage",
            statement_index,
            {"confidence": "explicit"},
        )
        if expression_node_id:
            builder.add_edge(
                "uses",
                input_col_id,
                expression_node_id,
                "Expression uses column",
                statement_index,
                {"function": lineage.get("type", "")},
            )


def _requires_expression_node(output_column: Dict[str, object]) -> bool:
    """Determine if an output column should have an expression node."""

    lineage = output_column.get("lineage", {})
    lineage_type = lineage.get("type", "")
    if lineage_type in {"direct", "column_rename"}:
        return False
    functions = lineage.get("functions", [])
    literals = lineage.get("literals", [])
    expression_sql = output_column.get("expression", "")
    return bool(
        functions
        or literals
        or "(" in expression_sql
        or "CASE" in expression_sql.upper()
    )


def _add_join_edges(
    builder: _GraphBuilder,
    sources: Iterable[Dict[str, str]],
    statement: Dict[str, object],
    statement_index: int,
    subquery_map: Dict[str, str],
) -> None:
    """Add join edges between tables."""

    source_tables = [src for src in sources if src.get("type") == "table"]
    left_source = source_tables[0] if source_tables else None
    for join in statement.get("joins", []) or []:
        right = join.get("right")
        if not right:
            continue
        right_name = right.get("name", "")
        right_id = _table_node_id_from_source_name(
            right_name, sources, statement_index, subquery_map
        )
        if left_source:
            left_name = left_source.get("name", "")
        else:
            left_name = "unknown"
        left_id = _table_node_id_from_source_name(
            left_name, sources, statement_index, subquery_map
        )
        builder.add_edge(
            "joins_with",
            left_id,
            right_id,
            "Tables joined",
            statement_index,
            {
                "join_condition": join.get("condition", ""),
                "join_type": join.get("join_type", ""),
            },
        )


def _add_union_edges(
    builder: _GraphBuilder,
    sources: Iterable[Dict[str, str]],
    statement: Dict[str, object],
    statement_index: int,
    target_table: Optional[Dict[str, str]],
) -> None:
    """Add union edges between sources and target."""

    if not statement.get("unions"):
        return
    if not target_table:
        return
    for source in sources:
        if source.get("type") != "table":
            continue
        source_id = table_id(source.get("name", "unknown"))
        builder.add_edge(
            "union_with",
            source_id,
            table_id(target_table["full_name"]),
            "Union input to target",
            statement_index,
            {"union_type": "union"},
        )


def _add_er_column_nodes(
    builder: _GraphBuilder,
    output_column: Dict[str, object],
    statement_index: int,
    target_table: Optional[Dict[str, str]],
    sources: Iterable[Dict[str, str]],
    subquery_map: Dict[str, str],
) -> None:
    """Add column nodes for ER mode."""

    target_full = target_table["full_name"] if target_table else "unknown"
    if target_full == "unknown":
        builder.add_node(
            {
                "id": table_id("unknown"),
                "type": "table",
                "name": "unknown",
                "database": "",
                "schema": "",
                "full_name": "unknown",
                "statement_index": statement_index,
                "description": "Unknown target table",
            }
        )
    output_name = output_column.get("name", "")
    output_col_id = column_id(target_full, output_name)
    builder.add_node(
        {
            "id": output_col_id,
            "type": "column",
            "table_id": table_id(target_full),
            "name": output_name,
            "data_type": None,
            "description": "Output column",
            "statement_index": statement_index,
            "literals": output_column.get("lineage", {}).get("literals", []),
        }
    )
    for input_ref in output_column.get("lineage", {}).get("inputs", []):
        resolved, warning = _resolve_with_subqueries(
            input_ref.get("table"), sources, statement_index, subquery_map
        )
        if warning:
            builder.add_warning(
                code="unresolved_table",
                message=warning,
                statement_index=statement_index,
                context=str(input_ref),
            )
        input_table = _resolved_full_name(resolved)
        input_col_id = column_id(input_table, input_ref.get("column", "unknown"))
        builder.add_node(
            {
                "id": input_col_id,
                "type": "column",
                "table_id": _table_node_id_from_resolved(resolved),
                "name": input_ref.get("column", "unknown"),
                "data_type": None,
                "description": "Input column",
                "statement_index": statement_index,
            }
        )


def _add_er_column_edges(
    builder: _GraphBuilder,
    output_columns: Iterable[Dict[str, object]],
    statement_index: int,
    sources: Iterable[Dict[str, str]],
    target_table: Optional[Dict[str, str]],
    subquery_map: Dict[str, str],
) -> None:
    """Add lineage edges for ER mode."""

    target_full = target_table["full_name"] if target_table else "unknown"
    for output_column in output_columns:
        output_name = output_column.get("name", "")
        output_col_id = column_id(target_full, output_name)
        lineage = output_column.get("lineage", {})
        how = lineage.get("mapping", [{}])[0].get("reason", lineage.get("type", ""))
        for input_ref in lineage.get("inputs", []):
            resolved, warning = _resolve_with_subqueries(
                input_ref.get("table"), sources, statement_index, subquery_map
            )
            if warning:
                builder.add_warning(
                    code="unresolved_table",
                    message=warning,
                    statement_index=statement_index,
                    context=str(input_ref),
                )
            input_table = _resolved_full_name(resolved)
            input_col_id = column_id(input_table, input_ref.get("column", "unknown"))
            builder.add_edge(
                "col_lineage",
                input_col_id,
                output_col_id,
                "Column lineage",
                statement_index,
                {
                    "how": how,
                    "expression_sql": output_column.get("expression", ""),
                },
            )


def _add_fk_like_edges(
    builder: _GraphBuilder,
    statement: Dict[str, object],
    statement_index: int,
    sources: Iterable[Dict[str, str]],
    subquery_map: Dict[str, str],
) -> None:
    """Add FK-like edges derived from join conditions."""

    for join in statement.get("joins", []) or []:
        condition = join.get("condition", "")
        if "=" not in condition:
            continue
        left, right = [part.strip() for part in condition.split("=", 1)]
        if "." not in left or "." not in right:
            continue
        left_table, left_col = left.split(".", 1)
        right_table, right_col = right.split(".", 1)
        left_resolved, left_warning = _resolve_with_subqueries(
            left_table, sources, statement_index, subquery_map
        )
        right_resolved, right_warning = _resolve_with_subqueries(
            right_table, sources, statement_index, subquery_map
        )
        if left_warning:
            builder.add_warning(
                code="unresolved_table",
                message=left_warning,
                statement_index=statement_index,
                context=left,
            )
        if right_warning:
            builder.add_warning(
                code="unresolved_table",
                message=right_warning,
                statement_index=statement_index,
                context=right,
            )
        left_id = column_id(_resolved_full_name(left_resolved), left_col)
        right_id = column_id(_resolved_full_name(right_resolved), right_col)
        if left_col.endswith("id") or right_col.endswith("id"):
            builder.add_edge(
                "fk_like",
                left_id,
                right_id,
                "FK-like join condition",
                statement_index,
                {"how": "join", "expression_sql": condition},
            )


def _ensure_table_columns(builder: _GraphBuilder) -> None:
    """Populate table nodes with column lists."""

    table_columns: Dict[str, List[str]] = {}
    for node in builder.nodes.values():
        if node.get("type") != "column":
            continue
        table_id_value = node.get("table_id", "")
        table_columns.setdefault(table_id_value, []).append(node.get("name", ""))
    for node in builder.nodes.values():
        if node.get("type") not in {"table", "cte", "subquery"}:
            continue
        node_id = node["id"]
        columns = ensure_unique_columns(table_columns.get(node_id, []))
        node["columns"] = columns


def _resolved_full_name(resolved: ResolvedTable) -> str:
    """Return the resolved full name for a source."""

    if resolved.source_type == "cte":
        return f"cte.{resolved.full_name}"
    if resolved.source_type == "subquery":
        return resolved.full_name
    return resolved.full_name


def _table_node_id_from_resolved(resolved: ResolvedTable) -> str:
    """Return table node identifier for a resolved source."""

    if resolved.source_type == "cte":
        return cte_id(resolved.full_name)
    if resolved.source_type == "subquery":
        return resolved.full_name
    if resolved.full_name == "unknown":
        return table_id("unknown")
    return table_id(resolved.full_name)


def _table_node_id_from_source_name(
    source_name: str,
    sources: Iterable[Dict[str, str]],
    statement_index: int,
    subquery_map: Dict[str, str],
) -> str:
    """Return table node identifier from a source name."""

    resolved, _warning = _resolve_with_subqueries(
        source_name, sources, statement_index, subquery_map
    )
    return _table_node_id_from_resolved(resolved)


def _build_subquery_map(
    statement_index: int, sources: Iterable[Dict[str, str]]
) -> Dict[str, str]:
    """Build a mapping of subquery aliases to node identifiers."""

    subquery_map: Dict[str, str] = {}
    counter = 0
    for source in sources:
        if source.get("type") != "subquery":
            continue
        counter += 1
        alias = source.get("name", f"subquery_{counter}")
        subquery_map[alias] = subquery_id(statement_index, counter)
    return subquery_map


def _resolve_with_subqueries(
    table_ref: Optional[str],
    sources: Iterable[Dict[str, str]],
    statement_index: int,
    subquery_map: Dict[str, str],
) -> Tuple[ResolvedTable, Optional[str]]:
    """Resolve table references while accounting for subquery aliases."""

    resolved, warning = resolve_table_reference(table_ref, sources)
    if resolved.source_type == "subquery":
        alias = resolved.full_name
        mapped = subquery_map.get(alias)
        if mapped:
            resolved = ResolvedTable(
                full_name=mapped, source_type=resolved.source_type, alias=resolved.alias
            )
        else:
            resolved = ResolvedTable(
                full_name=f"subquery:{statement_index}:{alias}",
                source_type=resolved.source_type,
                alias=resolved.alias,
            )
    return resolved, warning

"""Utility helpers for graph construction."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class ResolvedTable:
    """Resolved table metadata for graph construction."""

    full_name: str
    source_type: str
    alias: str


def normalize_full_name(database: str, name: str) -> str:
    """Build a normalized full table name."""

    if database:
        return f"{database}.{name}"
    return name


def table_id(full_name: str) -> str:
    """Create a stable table node identifier."""

    return f"table:{full_name}"


def cte_id(name: str) -> str:
    """Create a stable CTE node identifier."""

    return f"cte:{name}"


def subquery_id(statement_index: int, index: int) -> str:
    """Create a stable subquery node identifier."""

    return f"subquery:{statement_index}:{index}"


def column_id(table_full_name: str, column: str) -> str:
    """Create a stable column node identifier."""

    return f"column:{table_full_name}.{column}"


def expression_id(statement_index: int, output_name: str, expression: str) -> str:
    """Create a stable expression node identifier."""

    digest = hashlib.sha1(expression.encode("utf-8")).hexdigest()[:8]
    return f"expr:{statement_index}:{output_name}:{digest}"


def resolve_table_reference(
    table_ref: Optional[str], sources: Iterable[Dict[str, str]]
) -> Tuple[ResolvedTable, Optional[str]]:
    """Resolve a table reference to a full name and source type."""

    alias_map: Dict[str, Dict[str, str]] = {}
    name_map: Dict[str, Dict[str, str]] = {}
    for source in sources:
        name = source.get("name", "")
        alias = source.get("alias", "")
        alias_map[alias] = source
        name_map[name] = source
    if not table_ref:
        return (
            ResolvedTable(full_name="unknown", source_type="unknown", alias=""),
            "Missing table reference",
        )
    if table_ref in alias_map:
        source = alias_map[table_ref]
        return (
            ResolvedTable(
                full_name=source.get("name", table_ref),
                source_type=source.get("type", "table"),
                alias=source.get("alias", ""),
            ),
            None,
        )
    if table_ref in name_map:
        source = name_map[table_ref]
        return (
            ResolvedTable(
                full_name=source.get("name", table_ref),
                source_type=source.get("type", "table"),
                alias=source.get("alias", ""),
            ),
            None,
        )
    return (
        ResolvedTable(full_name=table_ref, source_type="unknown", alias=""),
        f"Unresolved table reference: {table_ref}",
    )


def split_table_name(full_name: str) -> Tuple[str, str]:
    """Split full table name into database and table name."""

    if "." in full_name:
        database, name = full_name.split(".", 1)
        return database, name
    return "", full_name


def ensure_unique_columns(columns: Iterable[str]) -> List[str]:
    """Return a list of unique columns preserving order."""

    seen = set()
    result: List[str] = []
    for column in columns:
        if column in seen:
            continue
        seen.add(column)
        result.append(column)
    return result

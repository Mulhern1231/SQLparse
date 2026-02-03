"""Context utilities for resolving column references."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from sqlglot import exp

from sql_lineage.models import ColumnRef


@dataclass
class SourceInfo:
    """Information about a source relation used in a query."""

    name: str
    alias: str
    database: str
    source_type: str
    output_inputs: Dict[str, List[ColumnRef]] = field(default_factory=dict)

    def identifier(self) -> str:
        """Return the identifier used to resolve columns for this source."""

        return self.alias or self.name


@dataclass
class AnalysisContext:
    """Analysis context holding sources and derived lineage mappings."""

    sources: List[SourceInfo]
    report_sources: List[SourceInfo]

    def resolve_source(self, name: str) -> Optional[SourceInfo]:
        """Resolve a source by alias or name."""

        for source in self.sources + self.report_sources:
            if source.identifier() == name:
                return source
        return None

    def candidate_sources(self) -> List[SourceInfo]:
        """Return sources that can satisfy unqualified columns."""

        non_cte_sources = [
            source for source in self.sources if source.source_type != "cte"
        ]
        if non_cte_sources:
            return non_cte_sources
        return list(self.sources)

    def resolve_unqualified_column(self) -> Optional[SourceInfo]:
        """Resolve an unqualified column if there is a single candidate source."""

        candidates = self.candidate_sources()
        if len(candidates) == 1:
            return candidates[0]
        return None


def build_source_info_from_table(table: exp.Table) -> SourceInfo:
    """Build source info from a table expression."""

    name = table.name
    database = table.db or ""
    alias = table.alias_or_name
    return SourceInfo(
        name=f"{database}.{name}".strip("."),
        alias=alias if alias != name else "",
        database=database,
        source_type="table",
    )


def build_source_info_from_subquery(
    alias: str, output_inputs: Dict[str, List[ColumnRef]]
) -> SourceInfo:
    """Build source info for a derived subquery."""

    return SourceInfo(
        name=alias,
        alias=alias,
        database="",
        source_type="subquery",
        output_inputs=output_inputs,
    )


def build_source_info_from_cte(
    name: str, output_inputs: Dict[str, List[ColumnRef]]
) -> SourceInfo:
    """Build source info for a CTE."""

    return SourceInfo(
        name=name,
        alias=name,
        database="",
        source_type="cte",
        output_inputs=output_inputs,
    )


def merge_sources(sources: List[SourceInfo]) -> List[SourceInfo]:
    """Deduplicate sources by type, name, and alias."""

    seen: set[Tuple[str, str, str]] = set()
    unique: List[SourceInfo] = []
    for source in sources:
        key = (source.source_type, source.name, source.alias)
        if key in seen:
            continue
        seen.add(key)
        unique.append(source)
    return unique

"""Data models for SQL lineage analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ColumnRef:
    """Reference to a column with an optional table qualifier."""

    table: Optional[str]
    column: str

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Serialize the column reference to a dictionary."""

        return {"table": self.table, "column": self.column}


@dataclass(frozen=True)
class LineageMapping:
    """Mapping from an output column to its input sources."""

    output_column: str
    sources: List[ColumnRef]
    reason: str

    def to_dict(self) -> Dict[str, object]:
        """Serialize the mapping to a dictionary."""

        return {
            "output_column": self.output_column,
            "sources": [source.to_dict() for source in self.sources],
            "reason": self.reason,
        }


@dataclass(frozen=True)
class LineageData:
    """Lineage metadata for an output column."""

    lineage_type: str
    inputs: List[ColumnRef]
    mapping: List[LineageMapping]
    functions: List[str]
    literals: List[str]
    notes: List[Dict[str, str]]

    def to_dict(self) -> Dict[str, object]:
        """Serialize lineage metadata to a dictionary."""

        return {
            "type": self.lineage_type,
            "inputs": [item.to_dict() for item in self.inputs],
            "mapping": [item.to_dict() for item in self.mapping],
            "functions": self.functions,
            "literals": self.literals,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class Dependency:
    """Dependency mapping for a column grouped by table."""

    table: str
    columns: List[str]

    def to_dict(self) -> Dict[str, object]:
        """Serialize dependency metadata to a dictionary."""

        return {"table": self.table, "columns": self.columns}


@dataclass(frozen=True)
class OutputColumn:
    """Metadata for an output column produced by a statement."""

    name: str
    expression: str
    lineage: LineageData
    dependencies: List[Dependency] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """Serialize output column metadata to a dictionary."""

        return {
            "name": self.name,
            "expression": self.expression,
            "lineage": self.lineage.to_dict(),
            "dependencies": [dependency.to_dict() for dependency in self.dependencies],
        }


@dataclass(frozen=True)
class StatementAnalysis:
    """Result of analyzing a single SQL statement."""

    index: int
    statement_type: str
    target: Optional[Dict[str, str]]
    output_columns: List[OutputColumn]
    sources: List[Dict[str, str]]
    joins: List[Dict[str, object]]
    unions: List[Dict[str, object]]
    subqueries: List[Dict[str, object]]
    errors: List[str]

    def to_dict(self) -> Dict[str, object]:
        """Serialize the statement analysis to a dictionary."""

        return {
            "index": self.index,
            "type": self.statement_type,
            "target": self.target,
            "output": {"columns": [col.to_dict() for col in self.output_columns]},
            "sources": self.sources,
            "joins": self.joins,
            "unions": self.unions,
            "subqueries": self.subqueries,
            "errors": self.errors,
        }

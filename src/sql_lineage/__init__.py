"""Public interface for the sql_lineage package."""

from __future__ import annotations

from sql_lineage.analyzer import analyze, to_json

__all__ = ["analyze", "to_json"]

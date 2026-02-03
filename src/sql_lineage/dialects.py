"""Dialect helpers for sql_lineage."""

from __future__ import annotations

from typing import List


SUPPORTED_DIALECTS = {"clickhouse", "postgres", "spark", "mysql"}


def normalize_dialect(dialect: str) -> str:
    """Normalize a dialect name to a lowercase string."""

    return dialect.strip().lower()


def is_supported_dialect(dialect: str) -> bool:
    """Return True if the dialect is explicitly supported."""

    return normalize_dialect(dialect) in SUPPORTED_DIALECTS


def supported_dialects() -> List[str]:
    """Return the list of supported dialects."""

    return sorted(SUPPORTED_DIALECTS)

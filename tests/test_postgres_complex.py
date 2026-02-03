from __future__ import annotations

from sql_lineage import analyze

POSTGRES_SQL = """
CREATE TABLE analytics.result_table AS
WITH base AS (
    SELECT
        u.id,
        u.email,
        o.total,
        COALESCE(o.discount, 0) AS discount
    FROM core.users u
    JOIN core.orders o
        ON u.id = o.user_id AND (o.status = 'paid' OR o.status = 'shipped')
),
enriched AS (
    SELECT
        id,
        email,
        total,
        discount,
        (total - discount) AS net_total
    FROM base
)
SELECT
    id AS user_id,
    email AS email,
    net_total,
    COALESCE(net_total, 0) AS net_total_filled,
    CASE WHEN net_total > 100 THEN 'vip' ELSE 'regular' END AS segment,
    total AS order_total
FROM enriched;
CREATE TABLE analytics.union_table AS
SELECT id AS user_id, email FROM core.users
UNION ALL
SELECT user_id AS user_id, email FROM core.orders;
"""


def _find_column(statement: dict, name: str) -> dict:
    """Locate an output column by name in a statement."""

    return next(col for col in statement["output"]["columns"] if col["name"] == name)


def test_postgres_multi_statement_parse() -> None:
    result = analyze(POSTGRES_SQL, dialect="postgres")
    assert result["errors"] == []
    assert result["dialect"] == "postgres"
    assert len(result["statements"]) == 2


def test_postgres_alias_and_coalesce_lineage() -> None:
    result = analyze(POSTGRES_SQL, dialect="postgres")
    statement = result["statements"][0]
    user_id_col = _find_column(statement, "user_id")
    assert user_id_col["lineage"]["type"] == "column_rename"
    assert user_id_col["lineage"]["mapping"][0]["reason"] == "alias"
    assert {"table": "u", "column": "id"} in user_id_col["lineage"]["inputs"]

    coalesce_col = _find_column(statement, "net_total_filled")
    assert "coalesce" in coalesce_col["lineage"]["functions"]
    assert "0" in coalesce_col["lineage"]["literals"]
    assert coalesce_col["lineage"]["mapping"][0]["reason"] == "coalesce"


def test_postgres_cte_and_dependencies() -> None:
    result = analyze(POSTGRES_SQL, dialect="postgres")
    statement = result["statements"][0]
    sources = statement["sources"]
    assert any(
        source["type"] == "cte" and source["name"] == "base" for source in sources
    )
    assert any(
        source["type"] == "cte" and source["name"] == "enriched" for source in sources
    )

    segment_col = _find_column(statement, "segment")
    dependencies = {dep["table"] for dep in segment_col["dependencies"]}
    assert "core.orders" in dependencies


def test_postgres_union_lineage() -> None:
    result = analyze(POSTGRES_SQL, dialect="postgres")
    statement = result["statements"][1]
    user_id_col = _find_column(statement, "user_id")
    assert user_id_col["lineage"]["type"] == "union"
    inputs = user_id_col["lineage"]["inputs"]
    assert {"table": "core.users", "column": "id"} in inputs
    assert {"table": "core.orders", "column": "user_id"} in inputs

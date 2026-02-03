from __future__ import annotations

from sql_lineage import analyze

SPARK_SQL = """
CREATE TABLE spark_analytics.users_flat
USING parquet
AS
SELECT
    u.id AS user_id,
    u.name AS user_name,
    exp_item.item AS item,
    named_struct('score', u.score, 'tier', u.tier) AS profile,
    COALESCE(u.region, 'na') AS region,
    CASE WHEN u.score > 90 THEN 'gold' ELSE 'standard' END AS tier_label
FROM spark_source.users u
LATERAL VIEW explode(u.items) exp_item AS item;
CREATE TABLE spark_analytics.user_metrics
USING parquet
AS
SELECT
    user_id,
    region,
    COUNT(1) AS total_items,
    MAX(score) AS max_score,
    MIN(score) AS min_score,
    COALESCE(AVG(score), 0) AS avg_score
FROM (
    SELECT
        u.id AS user_id,
        u.region,
        item.score AS score
    FROM spark_source.users u
    LATERAL VIEW explode(u.items) exp AS item
) metrics
GROUP BY user_id, region;
"""


def _find_column(statement: dict, name: str) -> dict:
    """Locate an output column by name in a statement."""

    return next(col for col in statement["output"]["columns"] if col["name"] == name)


def test_spark_multi_statement_parse() -> None:
    result = analyze(SPARK_SQL, dialect="spark")
    assert result["errors"] == []
    assert result["dialect"] == "spark"
    assert len(result["statements"]) == 2


def test_spark_coalesce_and_dependencies() -> None:
    result = analyze(SPARK_SQL, dialect="spark")
    statement = result["statements"][0]
    region_col = _find_column(statement, "region")
    assert "coalesce" in region_col["lineage"]["functions"]
    assert "na" in region_col["lineage"]["literals"]
    dependencies = {dep["table"] for dep in region_col["dependencies"]}
    assert "spark_source.users" in dependencies


def test_spark_nested_subquery_resolution() -> None:
    result = analyze(SPARK_SQL, dialect="spark")
    statement = result["statements"][1]
    avg_col = _find_column(statement, "avg_score")
    assert "coalesce" in avg_col["lineage"]["functions"]
    assert {"table": "metrics", "column": "score"} in avg_col["lineage"]["inputs"]

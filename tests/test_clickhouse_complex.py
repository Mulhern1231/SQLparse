from __future__ import annotations

from sql_lineage import analyze

CLICKHOUSE_SQL = """
CREATE OR REPLACE TABLE analytics.result_table
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT
    t.user_id AS user_id,
    t.event_date AS event_date,
    COALESCE(p.country, 'unknown') AS country,
    CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END AS amount,
    ifNull(p.segment, 'base') AS segment,
    toUInt32OrZero(splitByChar('_', t.session_id)[-1]) AS session_suffix
FROM analytics.raw_events t
LEFT JOIN (
    SELECT user_id, country, segment
    FROM analytics.user_profiles
) p ON p.user_id = t.user_id
WHERE t.event_date >= toDate('2024-01-01');
CREATE TABLE analytics.summary_table
ENGINE = MergeTree()
ORDER BY user_id
AS
SELECT
    user_id,
    count() AS event_count,
    sum(amount) AS total_amount,
    max(event_date) AS last_event_date,
    coalesce(country, 'unknown') AS last_country,
    min(session_suffix) AS min_suffix
FROM analytics.result_table
GROUP BY user_id, country;
"""


def _find_column(statement: dict, name: str) -> dict:
    """Locate an output column by name in a statement."""

    return next(col for col in statement["output"]["columns"] if col["name"] == name)


def test_clickhouse_multi_statement_parse() -> None:
    result = analyze(CLICKHOUSE_SQL, dialect="clickhouse")
    assert result["errors"] == []
    assert result["dialect"] == "clickhouse"
    assert len(result["statements"]) == 2


def test_clickhouse_coalesce_mapping() -> None:
    result = analyze(CLICKHOUSE_SQL, dialect="clickhouse")
    statement = result["statements"][0]
    country_col = _find_column(statement, "country")
    assert "coalesce" in country_col["lineage"]["functions"]
    assert "unknown" in country_col["lineage"]["literals"]
    assert country_col["lineage"]["mapping"][0]["reason"] == "coalesce"


def test_clickhouse_alias_and_dependencies() -> None:
    result = analyze(CLICKHOUSE_SQL, dialect="clickhouse")
    statement = result["statements"][0]
    user_id_col = _find_column(statement, "user_id")
    assert user_id_col["lineage"]["type"] == "column_rename"
    assert {"table": "t", "column": "user_id"} in user_id_col["lineage"]["inputs"]

    segment_col = _find_column(statement, "segment")
    dependencies = {dep["table"] for dep in segment_col["dependencies"]}
    assert "analytics.user_profiles" in dependencies

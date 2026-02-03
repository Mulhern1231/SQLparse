from __future__ import annotations

from sql_lineage import analyze

MULTI_SQL = """
CREATE TABLE analytics.first_table AS
SELECT
    a.id AS id,
    a.value AS value,
    COALESCE(a.flag, 0) AS flag_value,
    CASE WHEN a.status = 'ok' THEN 1 ELSE 0 END AS status_flag,
    b.name AS name,
    b.category AS category
FROM analytics.source_a a
JOIN analytics.source_b b ON a.id = b.id;
CREATE OR REPLACE TABLE analytics.second_table AS
SELECT
    id,
    name,
    category,
    value,
    flag_value,
    status_flag
FROM analytics.first_table;
"""


def test_multi_statement_count() -> None:
    result = analyze(MULTI_SQL, dialect="clickhouse")
    assert result["errors"] == []
    assert len(result["statements"]) == 2


def test_multi_statement_alias_and_dependencies() -> None:
    result = analyze(MULTI_SQL, dialect="clickhouse")
    statement = result["statements"][0]
    id_col = next(col for col in statement["output"]["columns"] if col["name"] == "id")
    assert id_col["lineage"]["type"] == "column_rename"
    assert {"table": "a", "column": "id"} in id_col["lineage"]["inputs"]

    dependencies = {dep["table"] for dep in id_col["dependencies"]}
    assert "analytics.source_a" in dependencies
    assert "analytics.source_b" in dependencies

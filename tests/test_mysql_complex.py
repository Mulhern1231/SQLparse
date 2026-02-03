from __future__ import annotations

from sql_lineage import analyze

MYSQL_SQL = """
CREATE TABLE mysql_analytics.result_one AS
SELECT
    u.id AS user_id,
    CONCAT(u.first_name, ' ', u.last_name) AS full_name,
    IFNULL(p.plan_name, 'free') AS plan_name,
    DATE_FORMAT(u.created_at, '%Y-%m-01') AS signup_month,
    COALESCE(a.city, 'unknown') AS city,
    CASE WHEN u.active = 1 THEN 'active' ELSE 'inactive' END AS status
FROM core.users u
LEFT JOIN core.plans p ON u.plan_id = p.id
LEFT JOIN (
    SELECT user_id, city
    FROM core.addresses
    WHERE active = 1
) a ON a.user_id = u.id;
CREATE TABLE mysql_analytics.result_two AS
SELECT
    user_id,
    plan_name,
    full_name,
    city
FROM mysql_analytics.result_one;
"""


def _find_column(statement: dict, name: str) -> dict:
    """Locate an output column by name in a statement."""

    return next(col for col in statement["output"]["columns"] if col["name"] == name)


def test_mysql_multi_statement_parse() -> None:
    result = analyze(MYSQL_SQL, dialect="mysql")
    assert result["errors"] == []
    assert result["dialect"] == "mysql"
    assert len(result["statements"]) == 2


def test_mysql_functions_and_dependencies() -> None:
    result = analyze(MYSQL_SQL, dialect="mysql")
    statement = result["statements"][0]
    full_name_col = _find_column(statement, "full_name")
    assert "concat" in full_name_col["lineage"]["functions"]

    plan_col = _find_column(statement, "plan_name")
    assert "ifnull" in plan_col["lineage"]["functions"]
    assert plan_col["lineage"]["mapping"][0]["reason"] == "expression"

    city_col = _find_column(statement, "city")
    assert "coalesce" in city_col["lineage"]["functions"]
    assert "unknown" in city_col["lineage"]["literals"]

    dependencies = {dep["table"] for dep in city_col["dependencies"]}
    assert "core.addresses" in dependencies
    assert "core.users" in dependencies


def test_mysql_alias_lineage() -> None:
    result = analyze(MYSQL_SQL, dialect="mysql")
    statement = result["statements"][0]
    user_id_col = _find_column(statement, "user_id")
    assert user_id_col["lineage"]["type"] == "column_rename"
    assert {"table": "u", "column": "id"} in user_id_col["lineage"]["inputs"]

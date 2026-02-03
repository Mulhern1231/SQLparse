from __future__ import annotations

from sql_lineage import analyze

SQL_QUERY = """
CREATE
OR REPLACE TABLE {{database}}.{{table_name}}
ENGINE = MergeTree()
ORDER BY tuple()
AS
SELECT COALESCE(r2.ch_schema, t.database) AS schema,
       COALESCE(r2.table_name, t.name) as table_name,
       u2.username AS username,
       u2.fio AS fio,
       u2.ss_user_id AS ss_user_id,
       toUInt32OrZero(splitByChar('_', COALESCE(r2.ch_schema, t.database))[-1]) AS ss_id,
       toUInt32OrZero(substring(t.name, 3, 7)) AS tsource_id,

       if(r2.dataset_name != '', r2.dataset_name, if(r3.dataset_name != '',r3.dataset_name, t.name)) AS dataset_name,
       if(r2.dataset_id = 0, r3.dataset_id, r2.dataset_id) AS dataset_id,
       COALESCE(r2.dataset_created_on, r3.dataset_created_on, analyst_ds.created_on) AS dataset_created_on,
       COALESCE(r2.dataset_changed_on, r3.dataset_changed_on, analyst_ds.changed_on) AS dataset_changed_on,

       if(r2.dashboard_id!= 0, r2.dashboard_id, r3.dashboard_id) AS dashboard_id,
       COALESCE(r2.dashboard_name,r3.dataset_name) AS dashboard_name,
       COALESCE(r2.dashboard_created_on, r3.dashboard_created_on) AS dashboard_created_on,
       COALESCE(r2.dashboard_changed_on, r3.dataset_changed_on) AS dashboard_changed_on,

       t.engine AS engine,
       t.metadata_modification_time AS modification_time,
       t.total_rows AS total_rows,
       t.total_bytes AS total_bytes,
       u2.org_name AS organization
FROM system.tables AS t
LEFT JOIN postgres_analytics_{{suffix}}.`tables` AS analyst_ds ON analyst_ds.table_name = t.name
LEFT JOIN ({{database}}).r2 ON (
    (r2.research_id = tsource_id AND tsource_id != 0) OR
    (r2.ss_user_id = analyst_ds.created_by_fk AND r2.ch_schema = analyst_ds.schema)
)
LEFT JOIN ({{database}}).r3 r3 ON (
    r3.table_name = t.name OR r3.dataset_name = analyst_ds.table_name
)
LEFT JOIN ({{database}}).users AS u2 ON
    u2.ss_user_id = toInt32(r2.ss_user_id) AND
    (
        u2.ss_user_id = toInt32(toUInt32OrZero(splitByChar('_', t.database)[-1])) AND
        toUInt32OrZero(splitByChar('_', t.database)[-1]) != 0
    )
    OR
    u2.ss_user_id = r3.ss_user_id
WHERE t.total_rows > 0
ORDER BY t.metadata_modification_time DESC;
"""


def test_query_parses() -> None:
    result = analyze(SQL_QUERY)
    assert result["errors"] == []


def test_sources_include_system_tables() -> None:
    result = analyze(SQL_QUERY)
    sources = result["sources"]
    assert any(
        source["name"].endswith("system.tables") and source["alias"] == "t"
        for source in sources
    )


def test_schema_lineage_contains_inputs() -> None:
    result = analyze(SQL_QUERY)
    columns = result["output"]["columns"]
    schema_col = next(col for col in columns if col["name"] == "schema")
    inputs = schema_col["lineage"]["inputs"]
    assert {"table": "r2", "column": "ch_schema"} in inputs
    assert {"table": "t", "column": "database"} in inputs


def test_join_detected() -> None:
    result = analyze(SQL_QUERY)
    joins = result["joins"]
    assert joins


def test_supported_dialects_parse_basic_query() -> None:
    query = "SELECT 1 AS one"
    for dialect in ["clickhouse", "postgres", "spark", "mysql"]:
        result = analyze(query, dialect=dialect)
        assert result["dialect"] == dialect
        assert result["errors"] == []

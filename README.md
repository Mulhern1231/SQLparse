# SQL Lineage

`sql-lineage` is a Python library that analyzes SQL and returns column-level lineage
information, including source tables, joins, expression dependencies, and
multi-statement analysis. Supported dialects include ClickHouse, PostgreSQL,
Spark SQL, and MySQL.

## Installation

```bash
pip install -e .
```

## Usage

```python
from sql_lineage import analyze, to_json

sql = """
CREATE TABLE analytics.result_table AS
SELECT a.id AS user_id, COALESCE(b.value, 0) AS value
FROM core.users a
LEFT JOIN core.metrics b ON a.id = b.user_id;
"""

result = analyze(sql, dialect="postgres")
statement = result["statements"][0]
print(statement["output"]["columns"][0]["lineage"])  # lineage for user_id

print(to_json(sql, dialect="postgres"))
```

### Output shape

```json
{
  "dialect": "postgres",
  "statements": [
    {
      "index": 1,
      "type": "create_table_as",
      "target": {"type": "table", "name": "result_table", "database": "analytics"},
      "output": {
        "columns": [
          {
            "name": "user_id",
            "expression": "a.id",
            "lineage": {
              "type": "column_rename",
              "inputs": [{"table": "a", "column": "id"}],
              "mapping": [
                {
                  "output_column": "user_id",
                  "sources": [{"table": "a", "column": "id"}],
                  "reason": "alias"
                }
              ],
              "functions": [],
              "literals": [],
              "notes": []
            },
            "dependencies": [{"table": "core.users", "columns": ["id"]}]
          }
        ]
      },
      "sources": [],
      "joins": [],
      "unions": [],
      "subqueries": [],
      "errors": []
    }
  ],
  "errors": []
}
```

## CLI

```bash
sql-lineage "SELECT a FROM t"
sql-lineage --file query.sql --dialect postgres
```

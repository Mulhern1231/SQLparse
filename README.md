# SQL Lineage

`sql-lineage` is a Python library that analyzes SQL and returns column-level lineage
information, including source tables, joins, and expression dependencies. Supported
dialects include ClickHouse, PostgreSQL, Spark SQL, and MySQL.

## Installation

```bash
pip install -e .
```

## Usage

```python
from sql_lineage import analyze, to_json

sql = "SELECT a, COALESCE(b, 1) AS b2 FROM t"
result = analyze(sql)
print(result["output"]["columns"][1]["lineage"])  # lineage for b2

print(to_json(sql))
```

## CLI

```bash
sql-lineage "SELECT a FROM t"
sql-lineage --file query.sql
```

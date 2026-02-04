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
from sql_lineage import analyze, build_graph, export_graph, to_json

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

graph = build_graph(sql, dialect="postgres", mode="full")
print(export_graph(graph, format="mermaid_flowchart"))
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

## Graph API

The graph API exposes a node/edge representation suitable for visualization and
post-processing.

### Inputs

* `sql`: SQL string (supports multi-statement inputs).
* `dialect`: SQL dialect string (defaults to `clickhouse`).
* `mode`: graph mode (`full`, `er_columns`, or `tables_only`).

### Outputs

`build_graph` returns a JSON-compatible dictionary:

* `dialect`: dialect used for parsing.
* `mode`: selected graph mode.
* `meta`: metadata (`statements`, `generated_at`, `library`, `version`).
* `nodes`: list of graph nodes.
* `edges`: list of graph edges.
* `errors`: list of non-fatal errors (e.g., unsupported export format).
* `warnings`: list of warnings with context and statement indices.

Each node includes:

* `id`: stable identifier (e.g., `table:core.users`).
* `type`: `table`, `column`, `expression`, `cte`, `subquery`, or `join` (if present).
* `description`: human-readable description.
* Optional metadata like `full_name`, `table_id`, `sql`, and `statement_index`.

Each edge includes:

* `id`: stable identifier (`edge:<type>:<n>`).
* `type`: `contains`, `uses`, `produces`, `lineage`, `joins_with`, `union_with`,
  `col_lineage`, or `table_lineage`.
* `from`/`to`: node identifiers.
* `description`: human-readable description.
* `statement_index`: statement index for grouping.
* `details`: additional metadata (join conditions, union types, confidence, etc.).

### Modes

* `full`: full dependency graph with tables, columns, expressions, CTEs, and
  subqueries. Includes `lineage` edges and expression dependencies.
* `er_columns`: ER-style graph with only tables and columns. Adds column lineage
  edges (`col_lineage`) and attempts FK-like edges from join conditions.
* `tables_only`: table-level lineage with aggregated edges and column counts.

### Export formats

`export_graph(graph, format=...)` supports:

* `json`: serialized JSON string.
* `mermaid_flowchart`: Mermaid flowchart for all modes.
* `mermaid_er`: Mermaid ER diagram (only for `er_columns` or `tables_only`).
* `graphviz_dot`: Graphviz DOT output with per-statement clusters.

If a format is incompatible with the chosen mode, the exporter records an error
in `errors` and falls back to Mermaid flowchart output.

## CLI

```bash
sql-lineage analyze --sql "SELECT a FROM t"
sql-lineage analyze --file query.sql --dialect postgres
sql-lineage graph --file query.sql --dialect postgres --mode full --format json
sql-lineage graph --sql "SELECT a FROM t" --mode er_columns --format mermaid_er
sql-lineage graph --sql "SELECT a FROM t" --mode tables_only --format graphviz_dot
```

## Русское описание

Ниже краткое описание на русском о том, что делает библиотека, какие данные
входят и выходят, а также какие режимы и форматы экспорта поддерживаются.

### Что на входе

* `sql`: строка SQL (можно несколько операторов).
* `dialect`: диалект SQL (по умолчанию `clickhouse`).
* `mode`: режим графа (`full`, `er_columns`, `tables_only`).

### Что на выходе

`build_graph` возвращает словарь:

* `dialect`, `mode`, `meta` — метаданные.
* `nodes` и `edges` — узлы и рёбра графа.
* `errors` и `warnings` — ошибки/предупреждения без падения.

Каждый узел и ребро содержит `description`, а идентификаторы стабильны
(`table:<full_name>`, `column:<table>.<column>`, и т.д.).

### Как работает (кратко)

Парсер разбирает SQL, находит источники/цели, колонки и выражения, строит
lineage-связи и упаковывает их в граф. Если часть источников не удаётся
разрешить, создаются `unknown`-узлы и добавляется предупреждение.

### Режимы

* `full` — полный граф: таблицы, колонки, выражения, CTE, подзапросы.
* `er_columns` — упрощённый ER-граф: таблицы и колонки + связи колонка→колонка.
* `tables_only` — только таблицы и агрегированные связи таблица→таблица.

### Экспорт

* `json` — JSON сериализация.
* `mermaid_flowchart` — Mermaid flowchart.
* `mermaid_er` — Mermaid ER (только `er_columns` и `tables_only`).
* `graphviz_dot` — Graphviz DOT.

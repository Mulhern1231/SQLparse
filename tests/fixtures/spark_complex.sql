CREATE TABLE analytics.spark_result USING parquet AS
SELECT
    u.id AS user_id,
    CONCAT(u.name, '-', o.category) AS label,
    COALESCE(o.amount, 0) AS amount
FROM users u
JOIN orders o
    ON u.id = o.user_id;
CREATE TABLE analytics.spark_items USING parquet AS
SELECT
    o.id,
    item.col AS item_name
FROM orders o
LATERAL VIEW explode(o.items) item AS col;

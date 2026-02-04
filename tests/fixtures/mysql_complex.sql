CREATE TABLE reporting.user_orders AS
SELECT
    u.id AS user_id,
    CONCAT(u.name, '-', IFNULL(o.type, 'unknown')) AS label,
    o.amount
FROM users u
JOIN orders o
    ON u.id = o.user_id;
CREATE TABLE reporting.union_table AS
SELECT id AS user_id, name FROM users
UNION ALL
SELECT user_id AS user_id, name FROM (
    SELECT user_id, name FROM orders
) sub;

CREATE TABLE analytics.result_table AS
WITH base AS (
    SELECT
        u.id,
        u.name,
        o.total,
        COALESCE(o.discount, 0) AS discount,
        o.status
    FROM core.users u
    JOIN core.orders o
        ON u.id = o.user_id AND o.status IN ('paid', 'shipped')
),
enriched AS (
    SELECT
        id,
        name,
        total,
        discount,
        (total - discount) AS net_total
    FROM base
)
SELECT
    id AS user_id,
    name AS user_name,
    net_total,
    COALESCE(net_total, 0) AS net_total_filled,
    name || '-' || status AS label
FROM enriched;
CREATE TABLE analytics.summary_table AS
SELECT
    user_id,
    SUM(net_total) AS sum_total
FROM analytics.result_table
GROUP BY user_id;
CREATE TABLE analytics.union_table AS
SELECT id AS user_id, name FROM core.users
UNION ALL
SELECT user_id AS user_id, name FROM core.orders;

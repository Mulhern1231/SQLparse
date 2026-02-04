CREATE OR REPLACE TABLE analytics.ch_result AS
SELECT
    u.id AS user_id,
    COALESCE(o.region, 'na') AS region,
    concat(u.name, '-', o.type) AS label
FROM core.users u
LEFT JOIN core.orders o
    ON u.id = o.user_id AND o.status IN ('paid', 'shipped');
CREATE OR REPLACE TABLE analytics.ch_summary AS
SELECT
    user_id,
    sum(amount) AS total_amount
FROM analytics.ch_result
GROUP BY user_id;

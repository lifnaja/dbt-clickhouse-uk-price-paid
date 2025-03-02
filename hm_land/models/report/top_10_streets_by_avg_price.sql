WITH top_10_streets_by_avg_price AS (
    SELECT
        street,
        avg_price
    FROM {{ ref('avg_sales_by_street') }}
    ORDER BY avg_price DESC
    LIMIT 10
)

SELECT * FROM top_10_streets_by_avg_price

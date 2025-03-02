
WITH avg_sales_by_street AS (
    SELECT
        street,
        AVG(price) AS avg_price
    FROM {{ ref('stg_uk_price_paid') }}
    GROUP BY street
)

SELECT * FROM avg_sales_by_street

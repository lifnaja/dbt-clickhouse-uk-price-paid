SELECT
    toYear(date) AS year,
    round(avg(price)) AS avg_price
FROM {{ ref('stg_uk_price_paid') }}
GROUP BY year
ORDER BY year
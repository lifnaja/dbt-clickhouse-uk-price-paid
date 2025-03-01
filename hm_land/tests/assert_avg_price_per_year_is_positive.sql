select
    year,
    avg_price
from {{ ref('avg_land_price_per_year') }}
where avg_price < 0
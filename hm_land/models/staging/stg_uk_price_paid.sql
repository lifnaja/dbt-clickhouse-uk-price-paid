WITH

source_data AS (
    SELECT
        uuid_string
        , price
        , date
        , postcode
        , property_type_flag
        , is_new
        , duration_flag
        , addr1
        , addr2
        , street
        , locality
        , town
        , district
        , county
        FROM {{ source('hm_land_registry', 'uk_price_paid') }}
)

, tranform_and_recast_date AS (
    SELECT
        uuid_string
        , toUInt32(price) as price
        , parseDateTimeBestEffortUS(date) as date
        , postcode
        , property_type_flag
        , is_new
        , duration_flag
        , addr1
        , addr2
        , street
        , locality
        , town
        , district
        , county
    FROM source_data
)

, add_fiscal_year_and_quarter AS (
    SELECT
        uuid_string
        , price as price
        , date as date
        , postcode
        , property_type_flag
        , is_new
        , duration_flag
        , addr1
        , addr2
        , street
        , locality
        , town
        , district
        , county
        , CASE
            WHEN toMonth(date) >= 10 
                THEN toYear(date) + 1
            ELSE toYear(date)
        END AS fiscal_year,
        CASE
            WHEN toMonth(date) BETWEEN 10 AND 12 THEN 1
            WHEN toMonth(date) BETWEEN 1 AND 3 THEN 2
            WHEN toMonth(date) BETWEEN 4 AND 6 THEN 3
            WHEN toMonth(date) BETWEEN 7 AND 9 THEN 4
        END AS fiscal_quarter
    FROM tranform_and_recast_date
)

SELECT
    *
FROM add_fiscal_year_and_quarter

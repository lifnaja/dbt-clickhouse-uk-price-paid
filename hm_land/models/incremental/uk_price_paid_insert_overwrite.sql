{{ 
    config(
        order_by='(uuid_string,addr1, addr2, street, locality, year)',
        engine='MergeTree()',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by='year'
    ) 
}}

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
        , toYear(date) AS year
        FROM {{ ref('stg_uk_price_paid') }}
        WHERE toYear(date) = 2018

    )

SELECT * FROM source_data

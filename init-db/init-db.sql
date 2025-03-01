CREATE DATABASE hm_land_registry;

CREATE TABLE hm_land_registry.uk_price_paid
(
    price String,
    date String,
    postcode LowCardinality(String),
    property_type_flag Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new Bool,
    duration_flag Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (addr1, addr2, street, locality);

INSERT INTO hm_land_registry.uk_price_paid
SELECT
    price,
    date_time,
    postcode,
    transform(property_type_flag, ['T', 'S', 'D', 'F', 'O'], ['terraced', 'semi-detached', 'detached', 'flat', 'other']) AS property_type,
    old_new_flag = 'Y' AS is_new,
    transform(duration_flag, ['F', 'L', 'U'], ['freehold', 'leasehold', 'unknown']) AS duration,
    addr1,
    addr2,
    street,
    locality,
    town,
    district,
    county
FROM url(
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{2015..2023}.csv',
    'CSV',
    'uuid_string String,
    price String,
    date_time String,
    postcode String,
    property_type_flag String,
    old_new_flag String,
    duration_flag String,
    addr1 String,
    addr2 String,
    street String,
    locality String,
    town String,
    district String,
    county String,
    ppd_category_type String,
    record_status String'
) SETTINGS max_http_get_redirects=10;
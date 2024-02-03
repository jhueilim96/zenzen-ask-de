WITH source AS(
SELECT * FROM {{ source('main', 'anime_bronze') }}
)
, extracted_to_list AS (
SELECT (source->>'Page'->>'media')::JSON[] as anime_list FROM source
)
, unnest_ AS (
SELECT unnest(anime_list) as anime FROM extracted_to_list
)
, extracted_attributes_list AS (
    SELECT
    json_extract_string(anime,
    [
        'id'
        , '$.externalLinks'

    ]) attribute_list
    FROM unnest_
)
, tb_links AS (
    SELECT
    attribute_list[1]::INT AS id
    , json_extract_string(unnest(attribute_list[2]::JSON[]),
    [
        '$.id'
        , '$.url'
        , '$.site'
        , '$.type'
        , '$.siteId'
        , '$.language'
    ]) attribute_list_link
    FROM extracted_attributes_list
)
SELECT
    attribute_list_link[1]::INT AS id
    , attribute_list_link[2]::VARCHAR AS url
    , attribute_list_link[3]::VARCHAR AS site
    , attribute_list_link[4]::VARCHAR AS type
    , attribute_list_link[5]::INT AS siteId
    , attribute_list_link[6]::VARCHAR AS language
    , id AS anime_id
FROM tb_links
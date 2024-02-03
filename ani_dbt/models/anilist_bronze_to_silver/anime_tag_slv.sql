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
    json_extract(anime,
    [
        'id'
        , '$.tags'

    ]) attribute_list
    FROM unnest_
)
, tb_tags AS (
    SELECT
    attribute_list[1]::INT AS id
    , json_extract_string(unnest(attribute_list[2]::JSON[]),
    [
        '$.id',
        '$.name'
    ]) attribute_list_tag
    FROM extracted_attributes_list
)
SELECT
    attribute_list_tag[1]::INT AS id
    , attribute_list_tag[2]::VARCHAR AS name
    , id AS anime_id
FROM tb_tags
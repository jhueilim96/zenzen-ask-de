WITH source AS (
    SELECT
        id as tag_id
        , anime_id

    FROM {{ref("anime_tag_slv")}}
)
SELECT * FROM source
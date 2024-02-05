WITH source AS (
    SELECT
        id as link_id
        , anime_id

    FROM {{ref("anime_link_slv")}}
)
SELECT * FROM source
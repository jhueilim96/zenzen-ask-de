WITH source AS (
    SELECT
        anime_id
        , character_id
        , role
    FROM {{ref("character_slv")}}
)
SELECT *
FROM source
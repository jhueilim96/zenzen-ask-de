WITH source AS (
    SELECT
        studio_edge_id
        , anime_id
        , studio_id
        , isMain
    FROM {{ref("studio_slv")}}
)
SELECT * FROM source
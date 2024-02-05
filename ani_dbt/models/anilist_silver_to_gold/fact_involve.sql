WITH source AS (
    SELECT
        staff_id
        , anime_id
        , staff_edge_id
        , role
    FROM {{ref("staff_slv")}}
)
SELECT * FROM source
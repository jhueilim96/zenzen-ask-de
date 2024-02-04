WITH source AS (
    SELECT
        studio_id
        , name
        , isAnimationStudio
    FROM {{ref("studio_slv")}}
),  dedupe AS (
    SELECT
        studio_id
        , ANY_VALUE(name) AS name
        , ANY_VALUE(isAnimationStudio) AS is_animation_studio
    FROM source
    GROUP BY (studio_id)
)
    SELECT * FROM dedupe
WITH source AS (
    SELECT
        id AS people_id
        , character_id
        , anime_id
    FROM {{ref("voice_actor_slv")}}
)
SELECT * FROM source
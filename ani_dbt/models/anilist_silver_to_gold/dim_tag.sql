WITH tb_filter_col AS (
    SELECT
        id AS tag_id
        , name
    FROM {{ref("anime_tag_slv")}}
), dedupe AS (
    SELECT
        tag_id
        , ANY_VALUE(name) AS name
    FROM tb_filter_col
    GROUP BY (tag_id )
)
SELECT *
FROM dedupe
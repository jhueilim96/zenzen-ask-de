WITH tb_filter_col AS (
    SELECT
        id AS url_id
        , url
        , site
        , type AS site_type
        , siteId AS site_id
        , language
    FROM {{ref("anime_link_slv")}}
), dedupe AS (
    SELECT
        url_id
        , ANY_VALUE(url) AS url
        , ANY_VALUE(site) AS site
        , ANY_VALUE(site_type) AS site_type
        , ANY_VALUE(site_id) AS site_id
        , ANY_VALUE(language) AS language
    FROM tb_filter_col
    GROUP BY (url_id )
)
SELECT *
FROM dedupe
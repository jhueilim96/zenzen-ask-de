WITH tb_filter_col AS (
    SELECT
    id AS anime_id
    , title
    , type
    , format
    , description
    , season
    , seasonYear
    , seasonInt
    , episodes
    , duration
    , chapters
    , volumes
    , countryOfOrigin
    , source
    , genres
    , synonyms
    , averageScore
    , meanScore
    , popularity
    , trending
    , favourites
    , isAdult
    , startDate
    , endDate
    FROM {{ ref('anime_slv') }}
), dedupe AS (
SELECT
    anime_id
    , ANY_VALUE(title) AS title
    , ANY_VALUE(type) AS type
    , ANY_VALUE(format) AS format
    , ANY_VALUE(description) AS description
    , ANY_VALUE(season) AS season
    , ANY_VALUE(seasonYear) AS seasonYear
    , ANY_VALUE(seasonInt) AS seasonInt
    , ANY_VALUE(episodes) AS episodes
    , ANY_VALUE(duration) AS duration
    , ANY_VALUE(chapters) AS chapters
    , ANY_VALUE(volumes) AS volumes
    , ANY_VALUE(countryOfOrigin) AS countryOfOrigin
    , ANY_VALUE(source) AS source
    , ANY_VALUE(genres) AS genres
    , ANY_VALUE(synonyms) AS synonyms
    , ANY_VALUE(averageScore) AS averageScore
    , ANY_VALUE(meanScore) AS meanScore
    , ANY_VALUE(popularity) AS popularity
    , ANY_VALUE(trending) AS trending
    , ANY_VALUE(favourites) AS favourites
    , ANY_VALUE(isAdult) AS isAdult
    , ANY_VALUE(startDate) AS startDate
    , ANY_VALUE(endDate) AS endDate
FROM tb_filter_col
GROUP BY (anime_id )
)
SELECT *
FROM dedupe

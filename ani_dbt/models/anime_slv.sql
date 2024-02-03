WITH source AS(
    SELECT * FROM {{ source('main', 'anime_bronze') }}
)
, extracted_to_list AS (
SELECT (source->>'Page'->>'media')::JSON[] as anime_list FROM source
)
, unnest_ AS (
SELECT unnest(anime_list) as anime FROM extracted_to_list
)
, extracted_attributes AS (
    SELECT json_extract_string(anime,
    [
        'id'
        , '$.title.english'
        , 'type'
        , 'format'
        , 'description'
        , 'season'
        , 'seasonYear'
        , 'seasonInt'
        , 'episodes'
        , 'duration'
        , 'chapters'
        , 'volumes'
        , 'countryOfOrigin'
        , 'source'
        , 'genres'
        , 'synonyms'
        , 'averageScore'
        , 'meanScore'
        , 'popularity'
        , 'trending'
        , 'favourites'
        , 'isAdult'
        , '$.startDate.year'
        , '$.startDate.month'
        , '$.startDate.day'
        , '$.endDate.year'
        , '$.endDate.month'
        , '$.endDate.day'
    ]) attribute_list
    FROM unnest_
)
, flatten_cast AS (
    SELECT
    attribute_list[1]::INT as id
    , attribute_list[2] as title
    , attribute_list[3] as type
    , attribute_list[4] as format
    , attribute_list[5] as description
    , attribute_list[6] as season
    , attribute_list[7]::INT as seasonYear
    , attribute_list[8]::INT as seasonInt
    , attribute_list[9]::INT as episodes
    , attribute_list[10]::INT as duration
    , attribute_list[11] as chapters
    , attribute_list[12] as volumes
    , attribute_list[13] as countryOfOrigin
    , attribute_list[14] as source
    , attribute_list[15]::JSON::VARCHAR[] as genres
    , attribute_list[16]::JSON::VARCHAR[] as synonyms
    , attribute_list[17]::INT as averageScore
    , attribute_list[18]::INT as meanScore
    , attribute_list[19]::INT as popularity
    , attribute_list[20]::INT as trending
    , attribute_list[21]::INT as favourites
    , attribute_list[22]::BOOL as isAdult
    , make_date(
        attribute_list[23]::INT
        , attribute_list[24]::INT
        , attribute_list[25]::INT
        ) as startDate
    , make_date(
        attribute_list[26]::INT
        , attribute_list[27]::INT
        , attribute_list[28]::INT
        ) as endDate
    FROM extracted_attributes
)
SELECT * from flatten_cast

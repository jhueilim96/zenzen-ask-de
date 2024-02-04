WITH source_va AS (
    SELECT
        id AS people_id
        , name_first
        , name_last
    FROM {{ref('voice_actor_slv')}}
), source_staff AS (
    SELECT
        staff_id AS people_id
        , name_first
        , name_middle
        , name_last
        , name_full
        , name_native
        , name_alternative
        , language
        , image_medium
        , description
        , primaryOccupations
        , gender
        , make_date(dateOfBirth_year, dateOfBirth_month, dateOfBirth_day) AS dateOfBirth
        --, make_date(dateOfDeath_year, dateOfDeath_month, dateOfDeath_day) AS dateOfDeath  -- Discard due to data quality
        , age
        , yearsActive
        , homeTown
        , bloodType
        , siteUrl
        FROM {{ref("staff_slv")}}
), source_union AS (
    SELECT * FROM source_va UNION ALL BY NAME SELECT * FROM source_staff
), dedupe AS (
    SELECT
        people_id
        , ANY_VALUE(name_first) AS name_first
        , ANY_VALUE(name_middle) AS name_middle
        , ANY_VALUE(name_last) AS name_last
        , ANY_VALUE(name_full) AS name_full
        , ANY_VALUE(name_native) AS name_native
        , ANY_VALUE(name_alternative) AS name_alternative
        , ANY_VALUE(language) AS language
        , ANY_VALUE(image_medium) AS image_medium
        , ANY_VALUE(description) AS description
        , ANY_VALUE(primaryOccupations) AS primaryOccupations
        , ANY_VALUE(gender) AS gender
        , ANY_VALUE(dateOfBirth) AS dateOfBirth
        , ANY_VALUE(age) AS age
        , ANY_VALUE(yearsActive) AS yearsActive
        , ANY_VALUE(homeTown) AS homeTown
        , ANY_VALUE(bloodType) AS bloodType
        , ANY_VALUE(siteUrl) AS siteUrl
    FROM source_union
    GROUP BY (people_id)
)
SELECT * FROM dedupe


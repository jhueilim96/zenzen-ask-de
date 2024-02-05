WITH tb_filter_col AS (
    SELECT
        character_id
        , name_first
        , name_middle
        , name_last
        , name_full
        , name_native
        , name_alternative
        , image
        , description
        , gender
        , make_date(dateOfBirth_year, dateOfBirth_month, dateOfBirth_day) AS dateOfBirth
        , age
        , bloodType
        , siteUrl
    FROM {{ ref("character_slv") }}
), dedupe AS (
    SELECT
        character_id
        , ANY_VALUE(name_first) AS name_first
        , ANY_VALUE(name_middle) AS name_middle
        , ANY_VALUE(name_last) AS name_last
        , ANY_VALUE(name_full) AS name_full
        , ANY_VALUE(name_native) AS name_native
        , ANY_VALUE(name_alternative) AS name_alternative
        , ANY_VALUE(image) AS image
        , ANY_VALUE(description) AS description
        , ANY_VALUE(gender) AS gender
        , ANY_VALUE(dateOfBirth) AS date_of_birth
        , ANY_VALUE(age) AS age
        , ANY_VALUE(bloodType) AS blood_type
        , ANY_VALUE(siteUrl) AS site_url
        , current_date AS last_updated
    FROM tb_filter_col
    GROUP BY (character_id )
)
SELECT *
FROM dedupe

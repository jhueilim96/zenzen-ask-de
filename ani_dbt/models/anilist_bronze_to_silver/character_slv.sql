WITH source AS(
    SELECT * FROM {{ source('main', 'character_bronze')  }}
)
, tb_edge_list AS (
    SELECT
    json_extract(source,
    [
        '$.data.Media.id'
        , '$.data.Media.characters.edges'
    ]) edges_list
    FROM source
)
, tb_unnest_edge_list AS (
    SELECT
        edges_list[1] AS anime_id
    ,   unnest(edges_list[2]::JSON[]) as edge
    FROM tb_edge_list
)
, tb_extract_edge AS (
    SELECT
    anime_id
    , json_extract_string(edge, [
        '$.role'
        , '$.name'
        , '$.node.id'
        , '$.node.name.first'
        , '$.node.name.middle'
        , '$.node.name.last'
        , '$.node.name.full'
        , '$.node.name.native'
        , '$.node.name.alternative'
        , '$.node.image.medium'
        , '$.node.description'
        , '$.node.gender'
        , '$.node.dateOfBirth.year'
        , '$.node.dateOfBirth.month'
        , '$.node.dateOfBirth.day'
        , '$.node.age'
        , '$.node.bloodType'
        , '$.node.siteUrl'
    ]) nodes
    FROM tb_unnest_edge_list
), rename AS(
    SELECT
    anime_id::INT as anime_id
    , nodes[1]::VARCHAR AS role
    , nodes[2]::VARCHAR AS role_name
    , nodes[3]::INT AS character_id
    , nodes[4]::VARCHAR AS name_first
    , nodes[5]::VARCHAR AS name_middle
    , nodes[6]::VARCHAR AS name_last
    , nodes[7]::VARCHAR AS name_full
    , nodes[8]::VARCHAR AS name_native
    , nodes[9]::JSON::VARCHAR[] AS name_alternative
    , nodes[10]::VARCHAR AS image
    , nodes[11]::VARCHAR AS description
    , nodes[12]::VARCHAR AS gender
    , nodes[13]::INT AS dateOfBirth_year
    , nodes[14]::INT  AS dateOfBirth_month
    , nodes[15]::INT  AS dateOfBirth_day
    , nodes[16]::VARCHAR  AS age
    , nodes[17]::VARCHAR AS bloodType
    , nodes[18]::VARCHAR AS siteUrl
    FROM tb_extract_edge
)
    SELECT *
    FROM rename
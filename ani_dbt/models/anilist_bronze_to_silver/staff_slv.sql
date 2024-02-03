WITH source AS(
    SELECT * FROM {{source('main', 'staff_bronze')}}
)
, tb_edge_list AS (
    SELECT
    json_extract(source,
    [
        '$.data.Media.id'
        , '$.data.Media.staff.edges'
    ]) edges_list
    FROM source
)
, tb_unnest_node_list AS (
    SELECT
        edges_list[1] AS anime_id
    ,   unnest(edges_list[2]::JSON[]) as edge
    FROM tb_edge_list
)
, tb_extract_node AS (
    SELECT
    anime_id
    , json_extract_string(edge, [
        '$.id'
        , '$.role'
        , '$.node.id'
        , '$.node.name.first'
        , '$.node.name.middle'
        , '$.node.name.last'
        , '$.node.name.full'
        , '$.node.name.native'
        , '$.node.name.alternative'
        , '$.node.languageV2'
        , '$.node.image.medium'
        , '$.node.description'
        , '$.node.primaryOccupations'
        , '$.node.gender'
        , '$.node.dateOfBirth.year'
        , '$.node.dateOfBirth.month'
        , '$.node.dateOfBirth.day'
        , '$.node.dateOfDeath.year'
        , '$.node.dateOfDeath.month'
        , '$.node.dateOfDeath.day'
        , '$.node.age'
        , '$.node.yearsActive'
        , '$.node.homeTown'
        , '$.node.bloodType'
        , '$.node.siteUrl'
    ]) nodes
    FROM tb_unnest_node_list
), rename AS(
    SELECT
    anime_id::INT as anime_id
    , nodes[1]::INT AS staff_edge_id
    , nodes[2]::VARCHAR AS role
    , nodes[3]::INT AS staff_id
    , nodes[4]::VARCHAR AS name_first
    , nodes[5]::VARCHAR AS name_middle
    , nodes[6]::VARCHAR AS name_last
    , nodes[7]::VARCHAR AS name_full
    , nodes[8]::VARCHAR AS name_native
    , nodes[9]::JSON::VARCHAR[] AS name_alternative
    , nodes[10]::VARCHAR AS language
    , nodes[11]::VARCHAR AS image_medium
    , nodes[12]::VARCHAR AS description
    , nodes[13]::JSON::VARCHAR[] AS primaryOccupations
    , nodes[14]::VARCHAR  AS gender
    , nodes[15]::INT  AS dateOfBirth_year
    , nodes[16]::INT  AS dateOfBirth_month
    , nodes[17]::INT  AS dateOfBirth_day
    , nodes[18]::INT  AS dateOfDeath_year
    , nodes[19]::INT  AS dateOfDeath_month
    , nodes[20]::INT  AS dateOfDeath_day
    , nodes[21]::INT  AS age
    , nodes[22]::INT[]  AS yearsActive
    , nodes[23]::VARCHAR  AS homeTown
    , nodes[24]::VARCHAR  AS bloodType
    , nodes[25]::VARCHAR AS siteUrl
    FROM tb_extract_node
)
    SELECT *
    FROM rename

WITH source AS(
    SELECT * FROM {{source('main', 'character_bronze')}}
)
, tb_edge_list AS (
    SELECT
    json_extract_string(source,
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
        , '$.voiceActors'
        , '$.node.id'
    ]) edges
    FROM tb_unnest_edge_list
), unnest_edge_voiceactor AS(
    SELECT
    anime_id as anime_id
    , edges[1] AS role
    , json_extract_string(unnest(edges[2]::JSON[]),[
        'id'
        , '$.name.first'
        , '$.name.last'
    ]) AS voiceActors
    , edges[3] as character_id
    FROM tb_extract_edge
), rename AS(
    SELECT
    anime_id
    , role
    , voiceActors[1] id
    , voiceActors[2] name_first
    , voiceActors[3] name_last
    , character_id
    FROM unnest_edge_voiceactor
), cast_variable AS(
SELECT
    id::INT AS id
    , name_first::VARCHAR AS name_first
    , name_last::VARCHAR AS name_last
    , anime_id::INT AS anime_id
    , role::VARCHAR AS role
    , character_id::INT AS character_id
FROM rename
)
SELECT *
FROM cast_variable

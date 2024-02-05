WITH source AS(
    SELECT * FROM {{source('main', 'studio_bronze')}}
)
, tb_edge_list AS (
    SELECT
    json_extract(source,
    [
        '$.data.Media.id'
        , '$.data.Media.studios.edges'
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
        , '$.isMain'
        , '$.node.id'
        , '$.node.name'
        , '$.node.isAnimationStudio'
    ]) nodes
    FROM tb_unnest_node_list
), rename AS(
    SELECT
    anime_id::INT as anime_id
    , nodes[1]::INT AS studio_edge_id
    , nodes[2]::BOOLEAN AS isMain
    , nodes[3]::INT AS studio_id
    , nodes[4]::VARCHAR AS name
    , nodes[5]::BOOLEAN AS isAnimationStudio
    FROM tb_extract_node
)
    SELECT *
    FROM rename

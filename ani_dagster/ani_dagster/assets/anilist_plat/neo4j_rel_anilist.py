from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext
)
from dagster_dbt import get_asset_key_for_model
from dagster_duckdb import DuckDBResource

from ..neo4j_utils import make_rel_list_from_table, make_node_list_from_table
from ...resources import Neo4jGraphResource
from .dbt_anilist import ani_dbt_assets

from . import neo4j_node_anilist as nd

import math

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'graph_has_link'), nd.anime_node, nd.link_node],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def has_link_rel(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql("""
        SELECT
            NULL AS rel_id
            , anime_id
            , link_id
        FROM graph_has_link
        """)
        source_label = "Anime"
        labels = 'HAS_LINK'
        target_label = "Link"
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels} relation (Source:{source_label}, Target:{target_label}) loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            rels = make_rel_list_from_table(batch, source_label, labels, tb.columns, target_label)
            res = neo4jgraph.merge_rel(rels, source_label, target_label)
            log.info(f"Merged {len(res)} rels in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Relation Label": labels,
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'graph_in_category'), nd.anime_node, nd.link_node],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def in_category_rel(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql("""
        SELECT
            NULL AS rel_id
            , anime_id
            , tag_id
        FROM graph_in_category
        """)
        source_label = "Anime"
        labels = 'IN_CATEGORY'
        target_label = "Tag"
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels} relation (Source:{source_label}, Target:{target_label}) loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            rels = make_rel_list_from_table(batch, source_label, labels, tb.columns, target_label)
            res = neo4jgraph.merge_rel(rels, source_label, target_label)
            log.info(f"Merged {len(res)} rels in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Relation Label": labels,
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'fact_involve'), nd.people_node, nd.anime_node],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def involve_rel(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql("""
        SELECT
            staff_edge_id AS rel_id
            , staff_id
            , anime_id
            , role
        FROM fact_involve
        """)
        labels = 'INVOLVE'
        source_label = "People"
        target_label = "Anime"
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels} relation (Source:{source_label}, Target:{target_label}) loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            rels = make_rel_list_from_table(batch, source_label, labels, tb.columns, target_label)
            res = neo4jgraph.merge_rel(rels, source_label, target_label)
            log.info(f"Merged {len(res)} rels in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Relation Label": labels,
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'fact_participate'), nd.character_node, nd.anime_node],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def participate_rel(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql("""
        SELECT
            NULL AS rel_id
            , character_id
            , anime_id
            , role
        FROM fact_participate
        """)
        labels = 'PARTICIPATE'
        source_label = "Character"
        target_label = "Anime"
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels} relation (Source:{source_label}, Target:{target_label}) loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            rels = make_rel_list_from_table(batch, source_label, labels, tb.columns, target_label)
            res = neo4jgraph.merge_rel(rels, source_label, target_label)
            log.info(f"Merged {len(res)} rels in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Relation Label": labels,
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'fact_produce'), nd.studio_node, nd.anime_node],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def produce_rel(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql("""
        SELECT
            studio_edge_id AS rel_id
            , studio_id
            , anime_id
            , isMain
        FROM fact_produce
        """)
        labels = 'PRODUCE'
        source_label = "Studio"
        target_label = "Anime"
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels} relation (Source:{source_label}, Target:{target_label}) loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            rels = make_rel_list_from_table(batch, source_label, labels, tb.columns, target_label)
            res = neo4jgraph.merge_rel(rels, source_label, target_label)
            log.info(f"Merged {len(res)} rels in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Relation Label": labels,
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'fact_voice_act'), nd.character_node, nd.people_node],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def voice_act_rel(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql("""
        SELECT
            NULL AS rel_id
            , people_id
            , character_id
        FROM fact_voice_act
        """)
        labels = 'VOICE_ACT'
        source_label = "People"
        target_label = "Character"
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels} relation (Source:{source_label}, Target:{target_label}) loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            rels = make_rel_list_from_table(batch, source_label, labels, tb.columns, target_label)
            res = neo4jgraph.merge_rel(rels, source_label, target_label)
            log.info(f"Merged {len(res)} rels in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Relation Label": labels,
            "Write Count": total_write
        })

@asset(
    deps=[nd.character_node],
    compute_kind='neo4j',
    group_name='neo4j_enrichment'
    )
def voice_act_rel(
    context: AssetExecutionContext,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    cypher = """
    MATCH (n:Character)
    WHERE n.site_url IS NOT NULL
    WITH n
    MERGE (n)-[:HAS_LINK]->(m:Link {url: n.site_url})
    return id(m) AS id
    """
    res = neo4jgraph.query(cypher, {})
    context.add_output_metadata(metadata={
        "Relation Label": 'HAS_LINK',
        "Source":"Character" ,
        "Target":"Link" ,
        "Write Count": len(res)
    })

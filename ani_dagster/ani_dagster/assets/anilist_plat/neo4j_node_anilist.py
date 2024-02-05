from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext
)
from dagster_dbt import get_asset_key_for_model
from dagster_duckdb import DuckDBResource

from ..neo4j_utils import make_rel_list_from_table, make_node_list_from_table
from ...resources import Neo4jGraphResource
import math
from .dbt_anilist import ani_dbt_assets


@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'dim_anime')],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def anime_node(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql('SELECT * from dim_anime')
        labels = ['Anime']
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels[0]} node loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            nodes = make_node_list_from_table(batch, labels, tb.columns)
            res = neo4jgraph.merge_node(nodes)
            log.info(f"Inserted {len(res)} nodes in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Node Label": labels[0],
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'dim_character')],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def character_node(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql('SELECT * from dim_character')
        labels = ['Character']
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels[0]} node loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            nodes = make_node_list_from_table(batch, labels, tb.columns)
            res = neo4jgraph.merge_node(nodes)
            log.info(f"Inserted {len(res)} nodes in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Node Label": labels[0],
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'dim_link')],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def link_node(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql('SELECT * from dim_link')
        labels = ['Link']
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels[0]} node loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            nodes = make_node_list_from_table(batch, labels, tb.columns)
            res = neo4jgraph.merge_node(nodes)
            log.info(f"Inserted {len(res)} nodes in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Node Label": labels[0],
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'dim_people')],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def people_node(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql('SELECT * from dim_people')
        labels = ['People']
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels[0]} node loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            nodes = make_node_list_from_table(batch, labels, tb.columns)
            res = neo4jgraph.merge_node(nodes)
            log.info(f"Inserted {len(res)} nodes in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Node Label": labels[0],
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'dim_studio')],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def studio_node(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql('SELECT * from dim_studio')
        labels = ['Studio']
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels[0]} node loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            nodes = make_node_list_from_table(batch, labels, tb.columns)
            res = neo4jgraph.merge_node(nodes)
            log.info(f"Inserted {len(res)} nodes in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Node Label": labels[0],
            "Write Count": total_write
        })

@asset(
    deps=[get_asset_key_for_model([ani_dbt_assets], 'dim_tag')],
    compute_kind='neo4j',
    group_name='neo4j'
    )
def tag_node(
    context: AssetExecutionContext,
    duck: DuckDBResource,
    neo4jgraph:Neo4jGraphResource
) -> None:
    log = get_dagster_logger()
    with duck.get_connection() as conn:
        tb = conn.sql('SELECT * from dim_tag')
        labels = ['Tag']
        BATCH_SIZE = 10000
        log.info(f"Initializing {labels[0]} node loading with upstream data {tb.shape}")
        total_write = 0
        for n in range(1, math.ceil(tb.shape[0]/BATCH_SIZE)+1):
            batch = tb.fetchmany(size=BATCH_SIZE)
            nodes = make_node_list_from_table(batch, labels, tb.columns)
            res = neo4jgraph.merge_node(nodes)
            log.info(f"Inserted {len(res)} nodes in batch {n} ")
            total_write += len(res)
        context.add_output_metadata(metadata={
            "Node Label": labels[0],
            "Write Count": total_write
        })

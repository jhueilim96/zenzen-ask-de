from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
)

from ...resources import TypesenseSearchIndexResource, Neo4jGraphResource
from ..anilist_plat import neo4j_rel_anilist as rel, neo4j_node_anilist as nd
from . import setup

from pathlib import Path
import pandas as pd


@asset(deps=[nd.link_node, rel.has_link_character], group_name="SearchIndex")
def update_webpage_index(
    context: AssetExecutionContext,
    typesense_: TypesenseSearchIndexResource,
    neo4jgraph: Neo4jGraphResource,
) -> None:
    BATCH_SIZE = 10000
    SEARCH_INDEX_NAME = 'webpage2'

    log = get_dagster_logger()
    client = typesense_.get_client()

    load_script_path = Path().cwd() / f"ani_dagster/assets/search_index/cypher_query/ser-webpage-retrieve.cypher"
    count_script_path = Path().cwd() / f"ani_dagster/assets/search_index/cypher_query/ser-webpage-count.cypher"

    with open(str(load_script_path), 'r') as fp:
        cypher_load = fp.read()
    with open(str(count_script_path), 'r') as fp:
        cypher_count = fp.read()

    res = neo4jgraph.query(cypher_count, {})
    entity_count = res[0].get('count_')
    log.info(f"Updating index:{SEARCH_INDEX_NAME} with {entity_count} data points")

    all_index_res = []
    for skip_count in range(0, entity_count, BATCH_SIZE):
        res = neo4jgraph.query(cypher_load, {'BATCH_SIZE':BATCH_SIZE, 'skip_count': skip_count})
        index_input_data = [doc.data() for doc in res]
        index_res = client.collections[SEARCH_INDEX_NAME].documents.import_(index_input_data, {'action': 'upsert'})
        all_index_res += index_res

    context.add_output_metadata(metadata={
        'Total Index Request': len(all_index_res),
        'Summary': MetadataValue.md(pd.DataFrame(all_index_res, ).loc[:, 'success'].value_counts().to_markdown()) ,
    })

@asset(deps=[nd.anime_node, setup.create_entity_index], group_name="SearchIndex")
def anime_in_entity_index(
    context: AssetExecutionContext,
    typesense_: TypesenseSearchIndexResource,
    neo4jgraph: Neo4jGraphResource,
) -> None:
    BATCH_SIZE = 10000
    SEARCH_INDEX_NAME = 'entity2'
    label = "Anime"
    log = get_dagster_logger()
    client = typesense_.get_client()

    load_script_path = Path().cwd() / f"ani_dagster/assets/search_index/cypher_query/{label}.cypher"
    with open(str(load_script_path), 'r') as fp:
        cypher_load = fp.read()

    res = neo4jgraph.query(f"MATCH (n:{label}) RETURN COUNT(n) as count_", {})
    entity_count = res[0].get('count_')
    log.info(f"Updating index:{SEARCH_INDEX_NAME} with {label} ({entity_count} node)")

    all_index_res = []
    for skip_count in range(0, entity_count, BATCH_SIZE):
        res = neo4jgraph.query(cypher_load, {'BATCH_SIZE':BATCH_SIZE, 'skip_count': skip_count})
        index_input_data = [doc.data() for doc in res]
        index_res = client.collections[SEARCH_INDEX_NAME].documents.import_(index_input_data, {'action': 'upsert'})
        all_index_res += index_res

    context.add_output_metadata(metadata={
        'Total Index Request': len(all_index_res),
        'Summary': MetadataValue.md(pd.DataFrame(all_index_res, ).loc[:, 'success'].value_counts().to_markdown()) ,
    })

@asset(deps=[nd.character_node, setup.create_entity_index], group_name="SearchIndex")
def character_in_entity_index(
    context: AssetExecutionContext,
    typesense_: TypesenseSearchIndexResource,
    neo4jgraph: Neo4jGraphResource,
) -> None:
    BATCH_SIZE = 10000
    SEARCH_INDEX_NAME = 'entity2'
    label = "Character"
    log = get_dagster_logger()
    client = typesense_.get_client()

    load_script_path = Path().cwd() / f"ani_dagster/assets/search_index/cypher_query/{label}.cypher"
    with open(str(load_script_path), 'r') as fp:
        cypher_load = fp.read()

    res = neo4jgraph.query(f"MATCH (n:{label}) RETURN COUNT(n) as count_", {})
    entity_count = res[0].get('count_')
    log.info(f"Updating index:{SEARCH_INDEX_NAME} with {label} ({entity_count} node)")

    all_index_res = []
    for skip_count in range(0, entity_count, BATCH_SIZE):
        res = neo4jgraph.query(cypher_load, {'BATCH_SIZE':BATCH_SIZE, 'skip_count': skip_count})
        index_input_data = [doc.data() for doc in res]
        index_res = client.collections[SEARCH_INDEX_NAME].documents.import_(index_input_data, {'action': 'upsert'})
        all_index_res += index_res

    context.add_output_metadata(metadata={
        'Total Index Request': len(all_index_res),
        'Summary': MetadataValue.md(pd.DataFrame(all_index_res, ).loc[:, 'success'].value_counts().to_markdown()) ,
    })

@asset(deps=[nd.people_node, setup.create_entity_index], group_name="SearchIndex")
def people_in_entity_index(
    context: AssetExecutionContext,
    typesense_: TypesenseSearchIndexResource,
    neo4jgraph: Neo4jGraphResource,
) -> None:
    BATCH_SIZE = 10000
    SEARCH_INDEX_NAME = 'entity2'
    label = "People"
    log = get_dagster_logger()
    client = typesense_.get_client()

    load_script_path = Path().cwd() / f"ani_dagster/assets/search_index/cypher_query/{label}.cypher"
    with open(str(load_script_path), 'r') as fp:
        cypher_load = fp.read()

    res = neo4jgraph.query(f"MATCH (n:{label}) RETURN COUNT(n) as count_", {})
    entity_count = res[0].get('count_')
    log.info(f"Updating index:{SEARCH_INDEX_NAME} with {label} ({entity_count} node)")

    all_index_res = []
    for skip_count in range(0, entity_count, BATCH_SIZE):
        res = neo4jgraph.query(cypher_load, {'BATCH_SIZE':BATCH_SIZE, 'skip_count': skip_count})
        index_input_data = [doc.data() for doc in res]
        index_res = client.collections[SEARCH_INDEX_NAME].documents.import_(index_input_data, {'action': 'upsert'})
        all_index_res += index_res

    context.add_output_metadata(metadata={
        'Total Index Request': len(all_index_res),
        'Summary': MetadataValue.md(pd.DataFrame(all_index_res, ).loc[:, 'success'].value_counts().to_markdown()) ,
    })

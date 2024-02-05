import os
from pathlib import Path
from dagster import (
    Definitions,
    EnvVar,
    AssetSelection,
    define_asset_job,
)
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

from .assets import (
    anilist_api_asset,
    anilist_bronze_asset,
    anilist_plat_asset,
    # anilist_dbt_asset,
    # anilist_neo4j_asset,
    # anime_node,
)

from .constants import dbt_project_dir
from .schedules import schedules
from .resources import QueueJsonFileSystemIOManager, Neo4jGraphResource


storage_path = Path().cwd().parent / 'raw/anilist/'
duckdb_path = Path().cwd().parent / 'ani_dbt/zenzen.duckdb'

all_assets = [
    *anilist_api_asset,
    *anilist_bronze_asset,
    *anilist_plat_asset
    # *anilist_dbt_asset,
    # *anilist_dbt_asset,
]

anilist_extract_job = define_asset_job("AnilistExtraction", selection=AssetSelection.assets(*anilist_api_asset))
anilist_load_transform_job = define_asset_job("AnilistLoadTransform",selection=(
    AssetSelection.assets(*anilist_bronze_asset)
))

all_jobs = [
    anilist_extract_job,
    anilist_load_transform_job,
]

defs = Definitions(
    assets=all_assets,
    schedules=schedules,
    jobs=all_jobs,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "q_json_fs": QueueJsonFileSystemIOManager(storage_dir=storage_path.as_posix()),
        'duck': DuckDBResource(database=str(duckdb_path)),
        'neo4jgraph': Neo4jGraphResource(uri=EnvVar('neo4j_uri'), username=EnvVar('neo4j_username'), password=EnvVar('neo4j_password'), db=EnvVar('neo4j_dbname'))
    },
)

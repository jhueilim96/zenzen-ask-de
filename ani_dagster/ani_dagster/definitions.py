import os
from pathlib import Path
from dagster import (
    Definitions,
    EnvVar,
    AssetSelection,
    define_asset_job,
    FilesystemIOManager,
    ScheduleDefinition,
)
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

from .assets import (
    anilist_api_asset,
    anilist_bronze_asset,
    anilist_plat_asset,
    anilist_search_index,
    rss_asset,
)

from .constants import dbt_project_dir
from .schedules import dbt_schedules
from .resources.core import (
    QueueJsonFileSystemIOManager,
    Neo4jGraphResource,
    TypesenseSearchIndexResource,
)
from .resources.rss import RssRegistry


storage_path = Path().cwd().parent / "raw/anilist/"
duckdb_path = Path().cwd().parent / "ani_dbt/zenzen.duckdb"

all_assets = [
    *anilist_api_asset,
    *anilist_bronze_asset,
    *anilist_plat_asset,
    *anilist_search_index,
    *rss_asset,
]

anilist_extract_job = define_asset_job(
    "AnilistExtract", selection=AssetSelection.assets(*anilist_api_asset)
)
anilist_load_transform_job = define_asset_job(
    "AnilistLoad", selection=(AssetSelection.assets(*anilist_bronze_asset))
)
datawarehouse_job = define_asset_job(
    "AnilistTransform_DW",
    selection=AssetSelection.assets(*anilist_plat_asset)
    - AssetSelection.groups("neo4j"),
)
neo4j_job = define_asset_job("Neo4j", selection=AssetSelection.groups("neo4j"))
index_job = define_asset_job(
    "IndexJob", selection=AssetSelection.assets(*anilist_search_index)
)
rss_extract_load_job = define_asset_job(
    "RssExtractLoad", selection=AssetSelection.groups("rss_extract_load")
)

all_jobs = [
    anilist_extract_job,
    anilist_load_transform_job,
    datawarehouse_job,
    neo4j_job,
    index_job,
    rss_extract_load_job,
]

rss_extract_load_schedule = ScheduleDefinition(
    job=rss_extract_load_job, cron_schedule="0 * * * *"
)

all_schedules = [
    *dbt_schedules,
    rss_extract_load_schedule,
]

defs = Definitions(
    assets=all_assets,
    schedules=all_schedules,
    jobs=all_jobs,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "q_json_fs": QueueJsonFileSystemIOManager(storage_dir=storage_path.as_posix()),
        "duck": DuckDBResource(database=str(duckdb_path)),
        "neo4jgraph": Neo4jGraphResource(
            uri=EnvVar("neo4j_uri"),
            username=EnvVar("neo4j_username"),
            password=EnvVar("neo4j_password"),
            db=EnvVar("neo4j_dbname"),
        ),
        "typesense_": TypesenseSearchIndexResource(
            host=EnvVar("typesense_host"),
            port=EnvVar("typesense_port"),
            protocol=EnvVar("typesense_protocol"),
            api_key=EnvVar("typesense_key"),
        ),
        "rss_registry": RssRegistry(
            path=(Path().cwd() / "ani_dagster/resources/rss_registry.yml").as_posix()
        ),
        "io_manager": FilesystemIOManager(),
    },
)

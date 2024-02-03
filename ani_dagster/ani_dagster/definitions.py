import os
from pathlib import Path
from dagster import (
    Definitions,
)
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

from .assets import (
    anilist_api_asset,
    anilist_bronze_asset,
    ani_dbt_dbt_assets
)

from .constants import dbt_project_dir
from .schedules import schedules
from .resources import QueueJsonFileSystemIOManager


storage_path = Path().cwd().parent / 'raw/anilist/'
duckdb_path = Path().cwd().parent / 'ani_dbt/zenzen.duckdb'

defs = Definitions(
    assets=[ani_dbt_dbt_assets, *anilist_api_asset, *anilist_bronze_asset],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "q_json_fs": QueueJsonFileSystemIOManager(storage_dir=storage_path.as_posix()),
        'duck': DuckDBResource(database=str(duckdb_path))
    },
)

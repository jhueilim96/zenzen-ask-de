from dagster import (
    asset,
    load_assets_from_package_module,
    load_assets_from_modules,
    get_dagster_logger,
    AssetExecutionContext,
)

from dagster_dbt import DbtCliResource, dbt_assets

from ..constants import dbt_manifest_path

from . import (
    anilist_api,
    anilist_lt,
    anilist_plat
)
from .anilist_plat.dbt_anilist import ani_dbt_assets
from .anilist_plat import neo4j_node_anilist

anilist_api_asset = load_assets_from_package_module(
    package_module=anilist_api,
    group_name='Anilist_E',
)

anilist_bronze_asset = load_assets_from_package_module(
    package_module=anilist_lt,
)

anilist_plat_asset = load_assets_from_package_module(
    package_module=anilist_plat,
)

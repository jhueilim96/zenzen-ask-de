from dagster import (
    load_assets_from_package_module,
    load_assets_from_modules,
    AssetExecutionContext,
)

from dagster_dbt import DbtCliResource, dbt_assets
from ..constants import dbt_manifest_path

from . import anilist_api

anilist_asset = load_assets_from_package_module(
    package_module=anilist_api,
    group_name='Anilist'
)


@dbt_assets(manifest=dbt_manifest_path)
def ani_dbt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
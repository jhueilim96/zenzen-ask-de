from dagster import (
    load_assets_from_package_module,
)

from . import (
    anilist_api,
    anilist_lt,
    anilist_plat,
    search_index,
    rss,
)

anilist_api_asset = load_assets_from_package_module(
    package_module=anilist_api,
    group_name="Anilist_E",
)

anilist_bronze_asset = load_assets_from_package_module(
    package_module=anilist_lt,
)

anilist_plat_asset = load_assets_from_package_module(
    package_module=anilist_plat,
)

anilist_search_index = load_assets_from_package_module(
    package_module=search_index,
)

rss_asset = load_assets_from_package_module(package_module=rss)

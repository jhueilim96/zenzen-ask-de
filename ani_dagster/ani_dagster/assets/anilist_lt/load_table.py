from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue
)
from dagster_duckdb import DuckDBResource

from pathlib import Path

@asset(compute_kind='python')
def anime_bronze(
    context: AssetExecutionContext,
    duck: DuckDBResource
):
    storage_path = Path().cwd().parent / 'raw/anilist/anime'

    with duck.get_connection() as conn:
        tb = conn.sql(f"SELECT * FROM read_json_auto('{str(storage_path)}/*.json')")
        bronze_table_name = 'anime_bronze'
        conn.sql(f'create or replace table {bronze_table_name} as select * from tb')
        context.add_output_metadata(metadata={
            "Row Count": tb.shape[0],
            "Preview": MetadataValue.md(tb.limit(1).df().to_markdown()),
            'Table': bronze_table_name
        })


@asset(compute_kind='python')
def character_bronze(
    context: AssetExecutionContext,
    duck: DuckDBResource
):
    storage_path = Path().cwd().parent / 'raw/anilist/character'

    with duck.get_connection() as conn:
        tb = conn.sql(f"SELECT * FROM read_json_auto('{str(storage_path)}/*.json')")
        bronze_table_name = 'character_bronze'
        conn.sql(f'create or replace table {bronze_table_name} as select * from tb')
        context.add_output_metadata(metadata={
            "Row Count": tb.shape[0],
            "Preview": MetadataValue.md(tb.limit(1).df().to_markdown()),
            'Table': bronze_table_name
        })

@asset(compute_kind='python')
def staff_bronze(
    context: AssetExecutionContext,
    duck: DuckDBResource
):
    storage_path = Path().cwd().parent / 'raw/anilist/staff'

    with duck.get_connection() as conn:
        tb = conn.sql(f"SELECT * FROM read_json_auto('{str(storage_path)}/*.json')")
        bronze_table_name = 'staff_bronze'
        conn.sql(f'create or replace table {bronze_table_name} as select * from tb')
        context.add_output_metadata(metadata={
            "Row Count": tb.shape[0],
            "Preview": MetadataValue.md(tb.limit(1).df().to_markdown()),
            'Table': bronze_table_name
        })

@asset(compute_kind='python')
def studio_bronze(
    context: AssetExecutionContext,
    duck: DuckDBResource
):
    storage_path = Path().cwd().parent / 'raw/anilist/studio'

    with duck.get_connection() as conn:
        tb = conn.sql(f"SELECT * FROM read_json_auto('{str(storage_path)}/*.json')")
        bronze_table_name = 'studio_bronze'
        conn.sql(f'create or replace table {bronze_table_name} as select * from tb')
        context.add_output_metadata(metadata={
            "Row Count": tb.shape[0],
            "Preview": MetadataValue.md(tb.limit(1).df().to_markdown()),
            'Table': bronze_table_name
        })




from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
)

from ...resources.core import TypesenseSearchIndexResource

import yaml
from pathlib import Path

def generate_drop_schema(schema):
    fields = schema.get("fields")
    return [
        {
            "name": field["name"],
            "drop": True,
        }
        for field in fields
    ]


@asset(group_name="SearchIndex")
def create_entity_index(
    context: AssetExecutionContext, typesense_: TypesenseSearchIndexResource
):
    schema_path = Path().cwd() / "ani_dagster/assets/search_index/index_schema.yml"
    with open(schema_path, "r") as fp:
        schemas = yaml.safe_load(fp)
    entity_index_schema = schemas["schema"][1]
    schema_name = entity_index_schema.get("name")
    client = typesense_.get_client()

    try:
        res = client.collections[schema_name].retrieve()
    except Exception:
        schema_exists = False
        res = client.collections.create(entity_index_schema)
    else:
        schema_exists = True

    context.add_output_metadata(metadata={"result": res, "Schema Exist": schema_exists})


def create_webpage_index():
    pass

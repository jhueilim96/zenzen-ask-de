from typing import Any
from dagster import (
    ConfigurableIOManager, InputContext, OutputContext,
    UPathIOManager,
    build_input_context,
    build_output_context,
    FilesystemIOManager,
    ConfigurableResource
)


from pathlib import Path
from dagster._core.execution.context.output import OutputContext
import json
from queue import Queue
import duckdb

class DuckdbJsonIOManager(ConfigurableResource):
    storage_dir:str

    def load_json_from_folder(self, conn:duckdb.DuckDBPyConnection) -> None:
        return None


class QueueJsonFileSystemIOManager(ConfigurableIOManager):

    storage_dir: str
    extension: str = '.json'

    def _get_path(self, context) -> str:
        return "/".join(self.storage_dir + context.asset_key.path)

    def handle_output(self, context: OutputContext, obj:Queue):

        q = obj

        while q.empty() == False:
            data, filename = q.get()
            file_path = Path(self.storage_dir) / (str(filename) + self.extension)
            with open(file_path, 'w') as fp:
                json.dump(data, fp)

    def load_input(self, context: InputContext, filename:str):
        pass

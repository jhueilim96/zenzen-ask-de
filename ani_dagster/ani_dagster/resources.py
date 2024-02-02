from dagster import (
    ConfigurableIOManager, InputContext, OutputContext,
    UPathIOManager,
    build_input_context,
    build_output_context,
    FilesystemIOManager
)


from pathlib import Path
from upath import UPath
import json
from queue import Queue



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
        file_path = Path(self.storage_dir) / (filename + self.extension)
        with open(file_path, 'r') as fp:
            data = json.load(fp)
        return data

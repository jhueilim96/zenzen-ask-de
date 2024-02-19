import yaml
from dagster import ConfigurableResource


class RssRegistry(ConfigurableResource):
    path: str

    def retrive_sites(self, site_key: str):
        with open(self.path, "r") as fp:
            all_sites = yaml.safe_load(fp)

        return all_sites.get(site_key)

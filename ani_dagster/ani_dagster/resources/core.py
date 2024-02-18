from dagster import (
    ConfigurableIOManager, InputContext, OutputContext,
    ConfigurableResource
)


from pathlib import Path
import json
from queue import Queue
import typesense
from neo4j import GraphDatabase

from ..assets.neo4j_utils import Node, Relation

class Neo4jGraphResource(ConfigurableResource):
    uri:str
    username:str
    password:str
    db:str

    def query(self, query:str, params:dict):

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password), database=self.db)
        eager_results = driver.execute_query(query_=query, parameters_=params)
        return eager_results.records

        # with GraphDatabase.session(database=self.db) as session:
        #     result = session.run(query, params)
        #     return [r for r in result]

    def merge_rel(self, relations:list[Relation], source_label:str, target_label:str):

        res = self.query(
            "UNWIND $data as row "
            f"MATCH (source_node:{source_label} {{ id: row.source.id }}) "
            f"MATCH (target_node:{target_label} {{ id: row.target.id }}) "
            "CALL apoc.merge.relationship(source_node, "
            "row.label, "
            "row.id, "
            "row.properties, "
            "target_node, "
            "row.properties ) "
            "YIELD rel "
            "RETURN rel.id AS id "
            ,
            {
                "data":[
                    {
                        'id': {'id': rel.id} if rel.id is not None else {},
                        'label': rel.label,
                        'source': rel.source.dict(),
                        'target':rel.target.dict(),
                        'properties': rel.properties,
                    } for rel in relations
                ],
            }
        )
        return res

    def merge_node(self, nodes:list[Node]):
        res = self.query(
            "UNWIND $data as row "
            "CALL apoc.merge.node("
            "row.label, "
            "{id:row.id}, "
            "row.properties, "
            "row.properties ) "
            "YIELD node "
            "RETURN node.id AS id"
            ,
            {
                "data":[
                    node.__dict__ for node in nodes
                ]
            }
        )
        return res


class TypesenseSearchIndexResource(ConfigurableResource):
    host:str
    port:str
    protocol:str
    api_key:str

    def get_client(self)->typesense.Client:
        client = typesense.Client({
            'nodes': [{
                'host': self.host,
                'port': self.port,
                'protocol': self.protocol
            }],
            'api_key': self.api_key,
            'connection_timeout_seconds': 2
        })
        return client

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
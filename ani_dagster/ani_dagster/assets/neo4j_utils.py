from pydantic import BaseModel
from typing import Any

class Node(BaseModel):
    id:int
    label: str | list[str]
    properties: dict[str, Any] = {}

class Relation(BaseModel):
    id:int | None
    label:str
    properties:dict[str, Any] = {}
    source: Node
    target:Node


def make_node_list_from_table(rows:list[Any], labels:list[str], columns:list[str])->list[Node]:
    nodes = []
    node_labels  = labels
    for row in rows:
        id_ = row[0]
        prop = dict(zip(columns[1:], row[1:]))
        node = Node(id=id_, label=node_labels, properties=prop)
        nodes.append(node)

    return nodes

def make_rel_list_from_table(rows:list[tuple], source_label:str, rel_labels:list[str], columns:list[str], target_label:str,)->list[Relation]:
    rels = []
    for row in rows:
        id_ = row[0]
        source_id = row[1]
        source = Node(id=source_id, label=source_label, properties={})
        target_id = row[2]
        target = Node(id=target_id, label=target_label, properties={})
        prop = dict(zip(columns[3:], row[3:])) if len(row) > 3 else {}
        rel = Relation(id=id_, label=rel_labels, properties=prop, source=source, target=target)
        rels.append(rel)

    return rels
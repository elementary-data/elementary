from typing import List, Literal, Optional, Tuple

import networkx as nx

from elementary.utils.pydantic_shim import BaseModel, validator

NodeUniqueIdType = str
NodeType = Literal["snapshot", "seed", "model", "source", "exposure"]
NodeSubType = Literal["table", "view"]


class LineageNodeSchema(BaseModel):
    id: NodeUniqueIdType
    type: NodeType
    sub_type: Optional[NodeSubType] = None


class LineageSchema(BaseModel):
    nodes: Optional[List[LineageNodeSchema]] = None
    edges: Optional[List[Tuple[NodeUniqueIdType, NodeUniqueIdType]]] = None

    @validator("nodes", pre=True, always=True)
    def set_nodes(cls, nodes):
        return nodes or []

    @validator("edges", pre=True, always=True)
    def set_edges(cls, edges):
        return edges or []

    def to_graph(self) -> nx.Graph:
        graph = nx.Graph()
        graph.add_edges_from(self.edges)
        return graph

    def to_directed_graph(self) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_edges_from(self.edges)
        return graph


class NodeDependsOnNodesSchema(BaseModel):
    unique_id: NodeUniqueIdType
    depends_on_nodes: Optional[List[NodeUniqueIdType]] = None
    type: NodeType
    sub_type: Optional[NodeSubType] = None

    @validator("depends_on_nodes", pre=True, always=True)
    def set_depends_on_nodes(cls, depends_on_nodes):
        formatted_depends_on = depends_on_nodes or []
        return [node_id for node_id in formatted_depends_on if node_id]

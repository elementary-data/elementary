from typing import List, Optional, Tuple

import networkx as nx
from pydantic import BaseModel, field_validator
from pydantic.typing import Literal

NodeUniqueIdType = str
NodeType = Literal["model", "source", "exposure"]


class LineageNodeSchema(BaseModel):
    id: NodeUniqueIdType
    type: NodeType


class LineageSchema(BaseModel):
    nodes: Optional[List[LineageNodeSchema]] = None
    edges: Optional[List[Tuple[NodeUniqueIdType, NodeUniqueIdType]]] = None

    @field_validator("nodes", mode="before")
    def set_nodes(cls, nodes):
        return nodes or []

    @field_validator("edges", mode="before")
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

from typing import List, Optional, Tuple
from pydantic import BaseModel, validator
from pydantic.typing import Literal
import networkx as nx
import re

SEED_PATH_PATERN = re.compile(r"^seed\.")

NodeUniqueIdType = str
NodeType = Literal["model", "source", "exposure"]


class NodeDependsOnNodesSchema(BaseModel):
    unique_id: NodeUniqueIdType
    depends_on_nodes: Optional[List[NodeUniqueIdType]] = None
    type: NodeType

    @validator("depends_on_nodes", pre=True, always=True)
    def set_depends_on_nodes(cls, depends_on_nodes):
        formatted_depends_on = depends_on_nodes or []
        formatted_depends_on = [
            cls._format_node_id(node_id) for node_id in formatted_depends_on
        ]
        return [node_id for node_id in formatted_depends_on if node_id]

    @classmethod
    def _format_node_id(cls, node_id: str):
        # Currently we don't save seeds in our artifacts.
        # We remove seeds from the lineage graph (as long as we don't support them).
        if re.search(SEED_PATH_PATERN, node_id):
            return None
        return node_id


class LineageNodeSchema(BaseModel):
    id: NodeUniqueIdType
    type: NodeType


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

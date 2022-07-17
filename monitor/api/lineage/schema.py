from typing import List, Optional, Tuple
from pydantic import BaseModel, validator
from pydantic.typing import Literal
import networkx as nx
import re

NodeUniqueIdType = str
NodeType = Literal["model", "source", "exposure"] 


class NodeDependsOnNodesSchema(BaseModel):
    unique_id: NodeUniqueIdType
    depends_on_nodes: Optional[List[NodeUniqueIdType]] = None
    type: NodeType

    @validator("depends_on_nodes", pre=True, always=True)
    def set_depends_on_nodes(cls, depends_on_nodes):
        formatted_depends_on = depends_on_nodes or []
        return [cls._format_node_id(node_id) for node_id in formatted_depends_on]
    
    @validator("unique_id", pre=True, always=True)
    def set_unique_id(cls, unique_id):
        return cls._format_node_id(unique_id)
    
    @classmethod
    def _format_node_id(cls, node_id: str):
        # Currently seeds are saved as sources.
        # Convert seed prefix into source for nodes unique ids.
        seed_patern = re.compile(r"^seed\.")
        if re.search(seed_patern, node_id):
            return re.sub(seed_patern, "source.", node_id, 1)
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

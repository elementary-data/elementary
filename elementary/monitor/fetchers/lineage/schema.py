import re
from typing import List, Optional

from pydantic import BaseModel, validator
from pydantic.typing import Literal

SEED_PATH_PATTERN = re.compile(r"^seed\.")

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
        if re.search(SEED_PATH_PATTERN, node_id):
            return None
        return node_id

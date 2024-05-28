from typing import List

import networkx as nx

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.monitor.api.lineage.schema import (
    LineageNodeSchema,
    LineageSchema,
    NodeDependsOnNodesSchema,
)
from elementary.monitor.fetchers.lineage.lineage import LineageFetcher


class LineageAPI(APIClient):
    def __init__(self, dbt_runner: BaseDbtRunner):
        super().__init__(dbt_runner)
        self.lineage_fetcher = LineageFetcher(dbt_runner=self.dbt_runner)

    def get_lineage(
        self,
        lineage_node_ids: List[str],
        exclude_elementary_models: bool = False,
    ) -> LineageSchema:
        lineage_graph = nx.DiGraph()
        nodes_depends_on_nodes = [
            node
            for node in self.lineage_fetcher.get_nodes_depends_on_nodes(
                exclude_elementary_models
            )
            if node.unique_id in lineage_node_ids
        ]
        for node_depends_on_nodes in nodes_depends_on_nodes:
            lineage_graph.add_edges_from(
                [
                    (node_depends_on_nodes.unique_id, depends_on_node)
                    for depends_on_node in (
                        node_depends_on_nodes.depends_on_nodes or []
                    )
                ]
            )
        return LineageSchema(
            nodes=self._convert_depends_on_node_to_lineage_node(nodes_depends_on_nodes),
            edges=list(lineage_graph.edges),
        )

    @staticmethod
    def _convert_depends_on_node_to_lineage_node(
        nodes_depends_on_nodes: List[NodeDependsOnNodesSchema],
    ) -> List[LineageNodeSchema]:
        return [
            LineageNodeSchema(
                id=node.unique_id,
                type=node.type,
                sub_type=node.sub_type,
            )
            for node in nodes_depends_on_nodes
        ]

from collections import defaultdict
from typing import List

import networkx as nx

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.monitor.api.lineage.schema import LineageNodeSchema, LineageSchema
from elementary.monitor.fetchers.lineage.lineage import LineageFetcher
from elementary.monitor.fetchers.lineage.schema import NodeDependsOnNodesSchema


class LineageAPI(APIClient):
    def __init__(self, dbt_runner: DbtRunner):
        super().__init__(dbt_runner)
        self.lineage_fetcher = LineageFetcher(dbt_runner=self.dbt_runner)

    def get_lineage(self, exclude_elementary_models: bool = False) -> LineageSchema:
        lineage_graph = nx.DiGraph()
        nodes_depends_on_nodes = self.lineage_fetcher.get_nodes_depends_on_nodes(
            exclude_elementary_models
        )
        for node_depends_on_nodes in nodes_depends_on_nodes:
            lineage_graph.add_edges_from(
                [
                    (node_depends_on_nodes.unique_id, depends_on_node)
                    for depends_on_node in node_depends_on_nodes.depends_on_nodes
                ]
            )
        return LineageSchema(
            nodes=self._convert_depends_on_node_to_lineage_node(nodes_depends_on_nodes),
            edges=list(lineage_graph.edges),
        )

    def get_dags(self) -> List[LineageSchema]:
        nodes_to_depends_map = defaultdict(list)
        lineage_graph = nx.Graph()

        nodes_depends_on_nodes = self._get_nodes_depends_on_nodes()
        for node_depends_on_nodes in nodes_depends_on_nodes:
            edges = [
                (node_depends_on_nodes.unique_id, depends_on_node)
                for depends_on_node in node_depends_on_nodes.depends_on_nodes
            ]
            nodes_to_depends_map[node_depends_on_nodes.unique_id] = edges
            lineage_graph.add_edges_from(edges)

        dags = []
        for connected_component in nx.connected_components(lineage_graph):
            dag_graph = nx.DiGraph()
            for node in connected_component:
                dag_graph.add_edges_from(nodes_to_depends_map[node])
            graph_nodes = list(dag_graph.nodes)
            lineage_nodes = [
                node for node in nodes_depends_on_nodes if node.unique_id in graph_nodes
            ]
            dags.append(
                LineageSchema(
                    nodes=self._convert_depends_on_node_to_lineage_node(lineage_nodes),
                    edges=list(dag_graph.edges),
                )
            )
        return dags

    @staticmethod
    def _convert_depends_on_node_to_lineage_node(
        nodes_depends_on_nodes: List[NodeDependsOnNodesSchema],
    ) -> List[LineageNodeSchema]:
        return [
            LineageNodeSchema(type=node.type, id=node.unique_id)
            for node in nodes_depends_on_nodes
        ]

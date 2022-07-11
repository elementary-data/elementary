import json
from typing import List
import networkx as nx
from collections import defaultdict

from clients.api.api import APIClient
from clients.dbt.dbt_runner import DbtRunner
from monitor.api.lineage.schema import LineageSchema, ModelDependsOnNodesSchema


class LineageAPI(APIClient):
    def __init__(self, dbt_runner: DbtRunner):
        super().__init__(dbt_runner)

    def get_lineage(self) -> LineageSchema:
        lineage_graph = nx.DiGraph()
        models_depends_on_nodes = self._get_models_depends_on_nodes()
        for model_depends_on_nodes in models_depends_on_nodes:
            lineage_graph.add_edges_from([(model_depends_on_nodes.unique_id, depends_on_node) for depends_on_node in model_depends_on_nodes.depends_on_nodes])
        return LineageSchema(
            nodes=[node for node in lineage_graph.nodes],
            edges=[edge for edge in lineage_graph.edges]
        )
    
    def get_dags(self) -> List[LineageSchema]:
        models_to_depends_map = defaultdict(list)
        lineage_graph = nx.Graph()

        models_depends_on_nodes = self._get_models_depends_on_nodes()
        for model_depends_on_nodes in models_depends_on_nodes:
            edges = [(model_depends_on_nodes.unique_id, depends_on_node) for depends_on_node in model_depends_on_nodes.depends_on_nodes]
            models_to_depends_map[model_depends_on_nodes.unique_id] = edges
            lineage_graph.add_edges_from(edges)

        dags = []
        for connected_component in nx.connected_components(lineage_graph):
            dag_graph = nx.DiGraph()
            for node in connected_component:
                dag_graph.add_edges_from(models_to_depends_map[node])
            dags.append(LineageSchema(
                nodes=[node for node in dag_graph.nodes],
                edges=[edge for edge in dag_graph.edges]
            ))
        return dags
    
    def _get_models_depends_on_nodes(self) -> List[ModelDependsOnNodesSchema]:
        models_depends_on_nodes = []
        models_depends_on_nodes_results = self.dbt_runner.run_operation(macro_name="get_models_depends_on_nodes")
        if models_depends_on_nodes_results:
            for model_depends_on_nodes_result in json.loads(models_depends_on_nodes_results[0]):
                models_depends_on_nodes.append(ModelDependsOnNodesSchema(
                    unique_id=model_depends_on_nodes_result.get("unique_id"),
                    depends_on_nodes=json.loads(model_depends_on_nodes_result.get("depends_on_nodes"))
                ))
        return models_depends_on_nodes

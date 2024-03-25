import json
from typing import List

from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.api.lineage.schema import NodeDependsOnNodesSchema

_MATERIALIZATION_TO_SUB_TYPE = {
    "view": "view",
    "table": "table",
    "incremental": "table",
}


class LineageFetcher(FetcherClient):
    def get_nodes_depends_on_nodes(
        self, exclude_elementary_models: bool = False
    ) -> List[NodeDependsOnNodesSchema]:
        nodes_depends_on_nodes = []
        nodes_depends_on_nodes_results = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_nodes_depends_on_nodes",
            macro_args={"exclude_elementary": exclude_elementary_models},
        )
        if nodes_depends_on_nodes_results:
            for node_depends_on_nodes_result in json.loads(
                nodes_depends_on_nodes_results[0]
            ):
                nodes_depends_on_nodes.append(
                    NodeDependsOnNodesSchema(
                        unique_id=node_depends_on_nodes_result.get("unique_id"),
                        depends_on_nodes=json.loads(
                            node_depends_on_nodes_result.get("depends_on_nodes")
                        )
                        if node_depends_on_nodes_result.get("depends_on_nodes")
                        else None,
                        type=node_depends_on_nodes_result.get("type"),
                        sub_type=self.get_node_sub_type(node_depends_on_nodes_result),
                    )
                )
        return nodes_depends_on_nodes

    @staticmethod
    def get_node_sub_type(node_depends_on_nodes_result: dict):
        materialization = node_depends_on_nodes_result.get("materialization")
        if materialization:
            return _MATERIALIZATION_TO_SUB_TYPE.get(materialization)

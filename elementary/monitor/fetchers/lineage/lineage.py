import json
from typing import List

from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.fetchers.lineage.schema import NodeDependsOnNodesSchema


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
                    )
                )
        return nodes_depends_on_nodes

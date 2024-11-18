import json
from typing import Dict, List, Optional, Set

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
        results = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_nodes_depends_on_nodes",
            macro_args={"exclude_elementary": exclude_elementary_models},
        )

        nodes = json.loads(results[0]) if results else []
        nodes = [self._normalize_result_dict(result) for result in nodes]
        id_to_node_map = {node["unique_id"]: node for node in nodes}
        return [
            NodeDependsOnNodesSchema(
                unique_id=node["unique_id"],
                depends_on_nodes=list(self._resolve_node_deps(node, id_to_node_map)),
                type=node["type"],
                sub_type=self.get_node_sub_type(node),
            )
            for node in nodes
            # Ephemeral models are not included in the lineage graph.
            if node["materialization"] != "ephemeral"
        ]

    @staticmethod
    def get_node_sub_type(node_depends_on_nodes_result: dict):
        materialization = node_depends_on_nodes_result["materialization"]
        if materialization:
            return _MATERIALIZATION_TO_SUB_TYPE.get(materialization)

    @staticmethod
    def _normalize_result_dict(result_dict: dict) -> dict:
        return {
            **result_dict,
            "depends_on_nodes": (
                json.loads(result_dict["depends_on_nodes"])
                if result_dict["depends_on_nodes"]
                else []
            ),
        }

    @classmethod
    def _resolve_node_deps(
        cls,
        node: dict,
        id_to_node_map: Dict[str, Dict],
        agg_deps: Optional[set] = None,
    ) -> Set[str]:
        agg_deps = agg_deps or set()
        dep_ids = node["depends_on_nodes"]
        for dep_id in dep_ids:
            dep_node = id_to_node_map.get(dep_id)
            if dep_node and dep_node["materialization"] == "ephemeral":
                agg_deps.update(
                    cls._resolve_node_deps(
                        dep_node,
                        id_to_node_map,
                        agg_deps,
                    )
                )
            else:
                agg_deps.add(dep_id)

        return agg_deps

import networkx as nx
from sqllineage import runner
from pyvis.network import Network
import webbrowser

# TODO: add types like a real pro
# TODO: add logger and debug logs like a pro
# TODO: add relevant requirements


class LineageGraph(object):
    def __init__(self):
        self.lineage_graph = nx.DiGraph()

    @staticmethod
    def _parse_query(query):
        try:
            # Very basic and naive parsing of source and target tables from query
            lineage_parsed_query = runner.LineageRunner(query)
            source_tables = lineage_parsed_query.source_tables
            target_tables = lineage_parsed_query.target_tables
        except Exception as e:
            return [], []

        return source_tables, target_tables

    def init_graph_from_query_list(self, queries, include_islands=False):
        for query in queries:
            source_tables, target_tables = self._parse_query(query)
            # if there is no source, add the target as a node
            if len(source_tables) == 0 and len(target_tables) > 0:
                if not include_islands:
                    continue
                for target_table in target_tables:
                    self.lineage_graph.add_node(str(target_table))
            # if there is no target, add the source as a node
            elif len(source_tables) > 0 and len(target_tables) == 0:
                if not include_islands:
                    continue
                for source_table in source_tables:
                    self.lineage_graph.add_node(str(source_table))
            else:
                # If both source and target exist, add a new edge to the graph
                for source in source_tables:
                    for target in target_tables:
                        print("Adding edge", str(source), str(target))
                        self.lineage_graph.add_node(str(source), shape='box')
                        self.lineage_graph.add_node(str(target), shape='box')
                        self.lineage_graph.add_edge(str(source), str(target))

    def draw_graph(self, should_open_browser=True):
        # Visualize the graph
        net = Network(height="100%", width="100%", directed=True, notebook=True)
        net.from_nx(self.lineage_graph)
        net.set_options("""{
            "edges": {
                "color": {
                    "inherit": true
                },
                "dashes": true,
                "smooth": {
                    "type": "continuous",
                    "forceDirection": "none"
                }
            },
            "layout": {
                "hierarchical": {
                    "enabled": true,
                    "levelSeparation": 485,
                    "nodeSpacing": 300,
                    "treeSpacing": 300,
                    "blockShifting": false,
                    "edgeMinimization": false,
                    "parentCentralization": false,
                    "direction": "LR",
                    "sortMethod": "directed"
                }
            },
            "interaction": {
                "navigationButtons": true
            },
            "physics": {
                "enabled": false,
                "hierarchicalRepulsion": {
                    "centralGravity": 0
                },
                "minVelocity": 0.75,
                "solver": "hierarchicalRepulsion"
            }
        }""")

        net.show("elementary_lineage.html")
        net.save_graph("elementary_lineage.html")
        if should_open_browser:
            webbrowser.open_new_tab('./elementary_lineage.html')
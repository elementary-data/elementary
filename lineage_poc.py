import snowflake.connector
import networkx as nx
from pyvis.network import Network
from sqllineage import runner
import os

# Connect to Snowflake
con = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT')
)

# Hardcoded history query
history_query = """
select query_text
  from table(elementary_db.information_schema.query_history(
    end_time_range_start=>to_timestamp_ltz('2021-08-26 00:00:00.000 -0700'),
    end_time_range_end=>to_timestamp_ltz('2021-08-26 12:30:00.000 -0700')));
"""

# Load recent queries from history log
queries = []
with con.cursor() as cursor:
    cursor.execute(history_query)
    rows = cursor.fetchall()
    for row in rows:
        queries.append(row[0])
        
# Create an empty graph
G=nx.DiGraph()
for query in queries:
    try:
        # Very basic and naive parsing of source and target tables from query
        lineage_parsed_query = runner.LineageRunner(query)
        source_tables = lineage_parsed_query.source_tables
        target_tables = lineage_parsed_query.target_tables
    except Exception as e:
        continue
    # if there is no source, add the target as a node
    if len(source_tables) == 0 and len(target_tables) > 0:
        continue
        for target_table in target_tables:
            G.add_node(str(target_table))
    # if there is no target, add the source as a node
    elif len(source_tables) > 0 and len(target_tables) == 0:
        continue
        for source_table in source_tables:
            G.add_node(str(source_table))
    else:
        # If both source and target exist, add a new edge to the graph
        for source in source_tables:
            for target in target_tables:
                print("Adding edge", str(source), str(target))
                G.add_edge(str(source), str(target))

# Visualize the graph
net = Network(height="100%", width="100%", directed=True, notebook=True)
net.from_nx(G)
net.set_options("""{
  "nodes": {
    "shape": "box",
    "size": 68
  },
  "edges": {
    "color": {
      "inherit": true
    },
    "dashes": true,
    "smooth": false
  },
  "layout": {
    "hierarchical": {
      "enabled": true,
      "levelSeparation": 450,
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
  "manipulation": {
    "enabled": false
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
#net.from_nx(nx.bfs_tree(G, 'elementary_db.elementary.customers'))
#net.barnes_hut()
#net.show_buttons(filter_=True)
net.show("elementary_lineage.html")
net.save_graph("elementary_lineage.html")

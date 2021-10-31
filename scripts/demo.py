from lineage.lineage_graph import LineageGraph


def main():
    lineage_graph = LineageGraph()
    lineage_graph.init_graph_from_edge_list([('salesforce_info_stg.lead_stage',
                                              'salesforce_info.lead_stage'),
                                             ('salesforce_info.lead_stage',
                                              'sales_reps.stages'),
                                             ('salesforce_info.lead_stage',
                                              'sales_leads.lead_stage'),
                                             ('salesforce_info.lead_stage',
                                              'sales_contracts.closed_won')])
    lineage_graph.draw_graph(enrich_with_monitoring=False)


if __name__ == '__main__':
    main()

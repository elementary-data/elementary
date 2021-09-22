import click
from pyfiglet import Figlet
from datetime import timedelta, date
from lineage.lineage_graph import LineageGraph
from lineage.query_history_factory import QueryHistoryFactory
from datetime import datetime

f = Figlet(font='slant')
print(f.renderText('Elementary'))


class RequiredIf(click.Option):
    def __init__(self, *args, **kwargs):
        self.required_if = kwargs.pop('required_if')
        assert self.required_if, "'required_if' parameter required"
        kwargs['help'] = (kwargs.get('help', '') +
                          ' NOTE: This argument must be configured together with %s.' %
                          self.required_if
                          ).strip()
        super(RequiredIf, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        we_are_present = self.name in opts
        other_present = self.required_if in opts

        if we_are_present and not other_present:
            raise click.UsageError(
                "Illegal usage: `%s` must be configured with `%s`" % (
                    self.name, self.required_if))
        else:
            self.prompt = None

        return super(RequiredIf, self).handle_parse_result(
            ctx, opts, args)


@click.command()
@click.option(
    '--start-date', '-s',
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]),
    default=str(date.today() - timedelta(days=6)),
    help="Parse queries in query log since this start date (you could also provide a specific time), "
         "default start date is 7 days ago."
)
@click.option(
    '--end-date', '-e',
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]),
    default=None,
    help="Parse queries in query log up to this end date (end_date is inclusive, "
         "you could also provide a specific time), default is current time."
)
@click.option(
    '--profiles-dir', '-d',
    type=click.Path(exists=True),
    required=True,
    help="You can connect to your data warehouse using your profiles dir, just specify your profiles dir where a "
         "profiles.yml is located (could be a dbt profiles dir).",
    cls=RequiredIf,
    required_if='profile_name'
)
@click.option(
    '--profile-name', '-p',
    type=str,
    required=True,
    help="The profile name of the chosen profile in your profiles file.",
    cls=RequiredIf,
    required_if='profiles_dir'
)
@click.option(
    '--open-browser', '-o',
    type=bool,
    default=True,
    help="Indicates if the data lineage graph should be opened in your default browser, if this flag is set to "
         "no-browser an html file will be saved to your current directory instead of opening it in your browser "
         "(by default the lineage will be opened automatically in your browser).",
)
@click.option(
    '--export-query-history', '-h',
    type=bool,
    default=False,
    help="Indicates if the query history pulled from your warehouse should be saved in your current "
         "directory to a file called latest_query_history.json (by default it won't be saved).",
)
@click.option(
    '--full-table-names', '-n',
    type=bool,
    default=False,
    help="Indicates if the lineage should display full table names including the relevant database and schema names "
         "(the default is to show only the table name)."
)
@click.option(
    '--ignore-schema', '-i',
    type=bool,
    default=False,
    help="Indicates if the lineage should filter only on tables in the configured profile's schema."
)
def main(start_date: datetime, end_date: datetime, profiles_dir: str, profile_name: str, open_browser: bool,
         export_query_history: bool, full_table_names: bool, ignore_schema: bool) -> None:
    """
    For more details check out our documentation here - https://docs.elementary-data.com/
    """
    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")
    query_history = QueryHistoryFactory(profiles_dir, profile_name, export_query_history).create_query_history()
    queries = query_history.extract_queries(start_date, end_date)
    lineage_graph = LineageGraph(show_isolated_nodes=False,
                                 profile_database_name=query_history.get_database_name(),
                                 profile_schema_name=query_history.get_schema_name() if not ignore_schema else None,
                                 full_table_names=full_table_names)
    lineage_graph.init_graph_from_query_list(queries)
    lineage_graph.draw_graph(should_open_browser=open_browser)


if __name__ == "__main__":
    main()

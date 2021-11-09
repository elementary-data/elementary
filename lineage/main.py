import click

from lineage.dbt_utils import extract_credentials_and_data_from_profiles
from lineage.empty_graph_helper import EmptyGraphHelper
from lineage.tracking import track_cli_start, track_cli_end, track_cli_exception
from lineage.exceptions import ConfigError
from pyfiglet import Figlet
from datetime import timedelta, date
from lineage.table_resolver import TableResolver
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
    default=True,
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
@click.option(
    '--table', '-t',
    type=str,
    help="Filter on a table to see upstream and downstream dependencies of this table (see also direction param)."
         " Table name format could be a full name like <db_name>.<schema_name>.<table_name>, a partial name like "
         "<schema_name>.<table_name> or only a table name <table_name>. If the database name wasn't part of the name "
         "the profiles database name will be used, if the schema name wasn't part of the name the profiles schema name "
         "will be used.",
    default=None
)
@click.option('--direction',
              type=click.Choice([LineageGraph.UPSTREAM_DIRECTION, LineageGraph.DOWNSTREAM_DIRECTION,
                                 LineageGraph.BOTH_DIRECTIONS]),
              help="Sets direction of dependencies when filtering on a specific table (default is both, "
                   "meaning showing both upstream and downstream dependencies of this table).",
              default='both',
              cls=RequiredIf,
              required_if='table')
@click.option('--depth',
              type=int,
              help="Sets how many levels of dependencies to show when filtering on a specific table "
                   "(default is showing all levels of dependencies).",
              default=None,
              cls=RequiredIf,
              required_if='table')
def main(start_date: datetime, end_date: datetime, profiles_dir: str, profile_name: str, open_browser: bool,
         export_query_history: bool, full_table_names: bool, ignore_schema: bool, table: str, direction: str,
         depth: int) -> None:
    """
    For more details check out our documentation here - https://docs.elementary-data.com/
    """
    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")

    credentials, profile_data = extract_credentials_and_data_from_profiles(profiles_dir, profile_name)
    anonymous_tracking = track_cli_start(profiles_dir, profile_data)

    try:
        query_history = QueryHistoryFactory(export_query_history, ignore_schema, full_table_names).\
            create_query_history(credentials, profile_data)
        queries = query_history.extract_queries(start_date, end_date)

        lineage_graph = LineageGraph(show_isolated_nodes=False)
        lineage_graph.init_graph_from_query_list(queries)

        if table is not None:
            table_resolver = TableResolver(profile_database_name=credentials.database,
                                           profile_schema_name=credentials.schema if not ignore_schema else None,
                                           queried_database_name=credentials.database,
                                           queried_schema_name=credentials.schema,
                                           full_table_names=full_table_names)
            resolved_table_name = table_resolver.name_qualification(table)
            if resolved_table_name is None:
                raise ConfigError(f'Could not resolve table name - {table}, please make sure to provide a table name'
                                  f'that is aligned with the database and schema in your profiles.yml file.')
            lineage_graph.filter_on_table(resolved_table_name, direction, depth)

        success = lineage_graph.draw_graph(should_open_browser=open_browser)
        if not success:
            print(EmptyGraphHelper(credentials.type).get_help_message())

        track_cli_end(anonymous_tracking, lineage_graph.properties(), query_history.properties())

    except Exception as exc:
        track_cli_exception(anonymous_tracking, exc)
        raise


if __name__ == "__main__":
    main()

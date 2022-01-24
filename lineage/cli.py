import click
import os
from os.path import expanduser

from lineage.utils import is_dbt_installed, get_package_version
from utils.dbt import extract_credentials_and_data_from_profiles
from lineage.empty_graph_helper import EmptyGraphHelper
from lineage.tracking import track_lineage_start, track_lineage_end, track_lineage_exception
from exceptions.exceptions import ConfigError
from datetime import timedelta, date
from lineage.table_resolver import TableResolver
from lineage.lineage_graph import LineageGraph
from lineage.query_history_factory import QueryHistoryFactory
from datetime import datetime


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


def get_cli_properties() -> dict:

    click_context = click.get_current_context()
    if click_context is None:
        return dict()

    params = click_context.params
    if params is None:
        return dict()

    start_date = params.get('start_date')
    end_date = params.get('end_date')
    is_filtered = params.get('table') is not None

    start_date_str = None
    if start_date is not None:
        start_date_str = start_date.isoformat()

    end_date_str = None
    if end_date is not None:
        end_date_str = end_date.isoformat()

    return {'start_date': start_date_str,
            'end_date': end_date_str,
            'is_filtered': is_filtered,
            'open_browser': params.get('open_browser'),
            'export_query_history': params.get('export_query_history'),
            'full_table_names': params.get('full_table_names'),
            'direction': params.get('direction'),
            'depth': params.get('depth'),
            'ignore_schema': params.get('ignore_schema'),
            'dbt_installed': is_dbt_installed(),
            'version': get_package_version()}


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
    '--database', '-db',
    type=str,
    required=True
)
@click.option(
    '--schema', '-sch',
    type=str,
    default=None
)
@click.option(
    '--config-dir', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr')
)
@click.option(
    '--profiles-dir', '-d',
    type=click.Path(exists=True),
    default=os.path.join(expanduser('~'), '.dbt'),
    help="You can connect to your data warehouse using your profiles dir, just specify your profiles dir where a "
         "profiles.yml is located (could be a dbt profiles dir).",
    cls=RequiredIf,
    required_if='profile_name'
)
@click.option(
    '--profile-name', '-p',
    type=str,
    default='edr',
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
def lineage(start_date: datetime, end_date: datetime, database: str, schema: str, config_dir: str,
            profiles_dir: str, profile_name: str, open_browser: bool, export_query_history: bool,
            full_table_names: bool, table: str, direction: str, depth: int) -> None:
    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")

    credentials, profile_data = extract_credentials_and_data_from_profiles(profiles_dir, profile_name)
    anonymous_tracking = track_lineage_start(config_dir, profiles_dir, profile_data, get_cli_properties())

    try:
        query_history_extractor = QueryHistoryFactory(database, schema, export_query_history, full_table_names).\
            create_query_history(credentials, profile_data)
        queries = query_history_extractor.extract_queries(start_date, end_date)

        lineage_graph = LineageGraph(show_isolated_nodes=False)
        lineage_graph.init_graph_from_query_list(queries)

        if table is not None:
            table_resolver = TableResolver(database_name=database,
                                           schema_name=schema,
                                           full_table_names=full_table_names)
            resolved_table_name = table_resolver.name_qualification(table)
            if resolved_table_name is None:
                raise ConfigError(f'Could not resolve table name - {table}, please make sure to provide a table name'
                                  f'that is aligned with the database and schema in your profiles.yml file.')
            lineage_graph.filter_on_table(resolved_table_name, direction, depth)

        success = lineage_graph.draw_graph(should_open_browser=open_browser)
        if not success:
            print(EmptyGraphHelper(credentials.type).get_help_message())

        track_lineage_end(anonymous_tracking, lineage_graph.properties(), query_history_extractor.properties())

    except Exception as exc:
        track_lineage_exception(anonymous_tracking, exc)
        raise


if __name__ == "__main__":
    lineage()

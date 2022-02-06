import click
import os
from os.path import expanduser

from config.config import Config
from lineage.query_history_factory import QueryHistoryFactory
from utils.package import get_package_version
from utils.dbt import is_dbt_installed
from lineage.empty_graph_helper import EmptyGraphHelper
from tracking.anonymous_tracking import track_cli_start, track_cli_end, track_cli_exception, AnonymousTracking
from exceptions.exceptions import ConfigError
from datetime import timedelta, date
from lineage.table_resolver import TableResolver
from lineage.lineage_graph import LineageGraph
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
            'command': click_context.command.name,
            'dbt_installed': is_dbt_installed(),
            'version': get_package_version()}


@click.group(invoke_without_command=True)
@click.option(
    '--database', '-db',
    type=str,
    help="Provide a database name to see the lineage for tables and views in this database."
)
@click.option(
    '--schema', '-sch',
    type=str,
    default=None,
    help="Filter on a schema to see tables and views only in this specific schema."
)
@click.option(
    '--table', '-t',
    type=str,
    help="Filter on a table to see upstream and downstream dependencies of this table "
         "(see also direction & depth parameters).",
    default=None
)
@click.option('--direction',
              type=click.Choice([LineageGraph.UPSTREAM_DIRECTION, LineageGraph.DOWNSTREAM_DIRECTION,
                                 LineageGraph.BOTH_DIRECTIONS]),
              help="Sets direction of dependencies when filtering on a specific table (default is both, "
                   "meaning showing both upstream and downstream dependencies of this table).",
              default='both',
              cls=RequiredIf,
              required_if='table'
)
@click.option('--depth',
              type=int,
              help="Sets how many levels of dependencies to show when filtering on a specific table "
                   "(default is showing all levels of dependencies).",
              default=None,
              cls=RequiredIf,
              required_if='table'
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
    '--full-table-names', '-n',
    type=bool,
    default=False,
    help="Indicates if the lineage should display full table names including the relevant database and schema names "
         "(the default is to show only the table name)."
)
@click.option(
    '--config-dir', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr'),
    help="Global settings for edr are configured in a config.yml file in this directory "
         "(if your config dir is HOME_DIR/.edr, no need to provide this parameter as we use it as default)."
)
@click.option(
    '--profiles-dir', '-d',
    type=click.Path(exists=True),
    default=os.path.join(expanduser('~'), '.dbt'),
    help="Specify your profiles dir where a profiles.yml is located, this could be a dbt profiles dir "
         "(if your profiles dir is HOME_DIR/.dbt, no need to provide this parameter as we use it as default).",
    cls=RequiredIf,
    required_if='profile_name'
)
@click.option(
    '--profile-name', '-p',
    type=str,
    default='elementary',
    help="The profile in your profiles.yml file that will be used as connection details to your data warehouse "
         "(if you configured 'elementary' profile in your profiles.yml, no need to provide this parameter as we use it "
         "as default).",
    cls=RequiredIf,
    required_if='profiles_dir'
)
@click.pass_context
def lineage(ctx, database: str, schema: str, table: str, direction: str, depth: int, open_browser: bool,
            full_table_names: bool, config_dir: str, profiles_dir: str, profile_name: str) -> None:
    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")
    if ctx.invoked_subcommand is not None:
        return

    config = Config(config_dir, profiles_dir, profile_name)
    anonymous_tracking = AnonymousTracking(config)
    track_cli_start(anonymous_tracking, 'lineage', get_cli_properties())

    try:

        lineage_graph = LineageGraph()
        lineage_graph.load_graph_from_file()

        if table is not None:
            table_resolver = TableResolver(database_name=database,
                                           schema_name=schema,
                                           full_table_names=full_table_names)
            resolved_table_name = table_resolver.name_qualification(table)
            if resolved_table_name is None:
                raise ConfigError(f'Could not resolve table name - {table}, please make sure to provide a table name '
                                  f'that is aligned with the database and schema parameters.\n'
                                  f'If you do not provide schema, add the schema as a prefix in the table filter - '
                                  f'edr lineage -db <database_name> -t <schema_name>.<table_name>')
            lineage_graph.filter_on_table(resolved_table_name, direction, depth)

        success = lineage_graph.draw_graph(should_open_browser=open_browser)
        if not success:
            print(EmptyGraphHelper(config.platform).get_help_message())

        execution_properties = lineage_graph.properties()
        track_cli_end(anonymous_tracking, 'lineage', execution_properties)

    except Exception as exc:
        track_cli_exception(anonymous_tracking, 'lineage', exc)
        raise


@lineage.command()
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
    '--databases', '-dbs',
    type=str,
    default=None,
    required=True,
    help=""
)
@click.option(
    '--config-dir', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr'),
    help="Global settings for edr are configured in a config.yml file in this directory "
         "(if your config dir is HOME_DIR/.edr, no need to provide this parameter as we use it as default)."
)
@click.option(
    '--profiles-dir', '-d',
    type=click.Path(exists=True),
    default=os.path.join(expanduser('~'), '.dbt'),
    help="Specify your profiles dir where a profiles.yml is located, this could be a dbt profiles dir "
         "(if your profiles dir is HOME_DIR/.dbt, no need to provide this parameter as we use it as default).",
    cls=RequiredIf,
    required_if='profile_name'
)
@click.option(
    '--profile-name', '-p',
    type=str,
    default='elementary',
    help="The profile in your profiles.yml file that will be used as connection details to your data warehouse "
         "(if you configured 'elementary' profile in your profiles.yml, no need to provide this parameter as we use it "
         "as default).",
    cls=RequiredIf,
    required_if='profiles_dir'
)
@click.pass_context
def generate(ctx, start_date: datetime, end_date: datetime, databases: str, config_dir: str,
             profiles_dir: str, profile_name: str):
    config = Config(config_dir, profiles_dir, profile_name)
    anonymous_tracking = AnonymousTracking(config)
    track_cli_start(anonymous_tracking, 'lineage', get_cli_properties())

    try:
        query_history_extractor = QueryHistoryFactory.create_query_history(config, databases)
        queries = query_history_extractor.extract_queries(start_date, end_date)

        lineage_graph = LineageGraph(show_isolated_nodes=False)
        lineage_graph.init_graph_from_query_list(queries)
        lineage_graph.export_graph_to_file()

        execution_properties = query_history_extractor.properties()
        execution_properties.update(lineage_graph.properties())

        #TODO: add 'generate' to the tracking
        track_cli_end(anonymous_tracking, 'lineage', execution_properties)
    except Exception as exc:
        #TODO: add 'generate' to the tracking
        track_cli_exception(anonymous_tracking, 'lineage', exc)
        raise


if __name__ == "__main__":
    lineage()

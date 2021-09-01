import os

import click
from pyfiglet import Figlet
from datetime import timedelta, date
from lineage.dbt_utils import connect_using_dbt_profiles
from lineage.query_history import extract_queries_from_query_history
from lineage.lineage_graph import LineageGraph
from lineage.utils import is_debug_mode_on

if not is_debug_mode_on():
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
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today() - timedelta(days=1)),
    help="Parse queries in query log since this start date, default start date is yesterday."
)
@click.option(
    '--end-date', '-e',
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today()),
    help="Parse queries in query log up to this end date, default end date is today."
)
@click.option(
    '--dbt-profiles-dir', '-d',
    type=click.Path(),
    default='',
    help="You can connect to your data warehouse using your dbt profiles file, just specify your dbt profiles dir and "
         "current profile name.",
    cls=RequiredIf,
    required_if='dbt_profile_name'
)
@click.option(
    '--dbt-profile-name', '-p',
    type=str,
    default='',
    help="You can connect to your data warehouse using your dbt profiles file, just specify your dbt profiles dir and "
         "current profile name.",
    cls=RequiredIf,
    required_if='dbt_profiles_dir'
)
@click.option(
    '--open-browser/--no-browser',
    type=bool,
    default=True,
    help="Indicate if the data lineage graph should be opened in your default browser, if this flag is set to "
         "no-browser an html file will be saved to your current directory instead of opening it in your browser.",
)
def main(start_date, end_date, dbt_profiles_dir, dbt_profile_name, open_browser):
    con = connect_using_dbt_profiles(dbt_profiles_dir, dbt_profile_name)
    queries = extract_queries_from_query_history(con, start_date, end_date)
    lineage_graph = LineageGraph(show_islands=False)
    lineage_graph.init_graph_from_query_list(queries)
    lineage_graph.draw_graph(should_open_browser=open_browser)


if __name__ == "__main__":
    main()

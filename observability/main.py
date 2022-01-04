import click
from pyfiglet import Figlet
from datetime import date, timedelta
import snowflake.connector

from build.lib.lineage.exceptions import ConfigError
from lineage.dbt_utils import extract_credentials_and_data_from_profiles
from genson import SchemaBuilder
import json
from tqdm import tqdm
from json_schema_for_humans.generate import generate_from_filename, generate_from_schema
from json_schema_for_humans.generation_configuration import GenerationConfiguration
import webbrowser
import yaml
import os
import subprocess

snowflake.connector.paramstyle = 'numeric'

f = Figlet(font='slant')
print(f.renderText('Elementary'))

ELEMENTARY_DBT_PACKAGE_NAME = 'elementary_observability'
ELEMENTARY_DBT_PACKAGE = 'git@github.com:elementary-data/elementary-dbt.git'
ELEMENTARY_DBT_PACKAGE_VERSION = 'master'


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


def write_schema_json_to_output_files(json_schema: dict) -> None:
    with open("my_schema.json", 'w') as schema_json_file:
        schema_json_file.write(json.dumps(json_schema))
    with open('my_schema.yml', 'w') as schema_yml_file:
        yaml.dump(json_schema, schema_yml_file, allow_unicode=True)


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def elementary_data_reliability(ctx, debug):
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug


@elementary_data_reliability.command()
@click.pass_context
def init(ctx):
    if not os.path.exists('dbt_project.yml'):
        # TODO: Exceptions should be in a common module and not here
        raise ConfigError('Please run this command from your main dbt project')

    #TODO: replace with dbt hub name and latest version once we deploy to dbt hub

    elementary_dbt_package = {'git': ELEMENTARY_DBT_PACKAGE,
                              'revision': ELEMENTARY_DBT_PACKAGE_VERSION}

    print("Adding elementary data reliability package to your dbt packages\n")
    dbt_packages = {'packages': []}
    if os.path.exists('packages.yml'):
        with open('packages.yml', 'r') as packages_file:
            dbt_packages = yaml.load(packages_file)

    current_installed_packages = {package.get('package') or package.get('git') for package in
                                  dbt_packages.get('packages', [])}

    if ELEMENTARY_DBT_PACKAGE not in current_installed_packages:
        dbt_packages['packages'].append(elementary_dbt_package)
        with open('packages.yml', 'w') as packages_file:
            yaml.dump(dbt_packages, packages_file)

    print("Running 'dbt deps' to download the elementary data reliability package\n")
    dbt_deps_result = subprocess.run(["dbt", "deps"], check=False, capture_output=True)
    if dbt_deps_result.returncode != 0:
        print(dbt_deps_result.stdout.decode('utf-8'))
        return

    print("Adding elementary data reliability package to your dbt_project.yml\n")
    with open('dbt_project.yml', 'r') as dbt_project_file:
        dbt_project = yaml.load(dbt_project_file)

    dbt_project['models'] = dbt_project.get('models', {}).update({ELEMENTARY_DBT_PACKAGE_NAME: {'+enabled': False}})
    with open('dbt_project.yml', 'w') as dbt_project_file:
        yaml.dump(dbt_project, dbt_project_file)

    print("Running 'dbt deps' to download elementary data reliability package dependencies\n")
    os.chdir(os.path.join('dbt_packages', ELEMENTARY_DBT_PACKAGE_NAME))
    dbt_deps_result = subprocess.run(["dbt", "deps"], check=False, capture_output=True)
    if dbt_deps_result.returncode != 0:
        print(dbt_deps_result.stdout.decode('utf-8'))
        return

    print("Elementary data reliability package was successfully added to your dbt project.\n"
          "Please make sure to commit your packages.yml and dbt_project.yml files.")


@elementary_data_reliability.command()
@click.pass_context
@click.option(
    '--profiles-dir', '-d',
    type=click.Path(exists=True),
    help="You can connect to your data warehouse using your profiles dir, just specify your profiles dir where a "
         "profiles.yml is located (could be a dbt profiles dir)."
)
@click.option(
    '--profile-name', '-p',
    type=str,
    help="The profile name of the chosen profile in your profiles file."
)
@click.option(
    '--table-name', '-t',
    type=str,
    default=None
)
@click.option(
    '--json-column-name', '-j',
    type=str,
    default=None
)
@click.option(
    '--time-column', '-c',
    type=str,
    default=None
)
@click.option(
    '--start-time', '-s',
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]),
    default=str(date.today() - timedelta(days=6))
)
@click.option(
    '--file-name', '-f',
    type=str,
    default=None
)
def profile_sources(ctx, profiles_dir, profile_name, table_name, json_column_name, time_column, start_time, file_name):
    click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")

    builder = SchemaBuilder()
    if file_name is not None:
        with open(file_name, 'r') as jsons_file:
            rows = jsons_file.readlines()
            for row in tqdm(rows, desc=f"Extracting and parsing jsons from {file_name}", colour='green'):
                builder.add_object(json.loads(row))
        write_schema_json_to_output_files(builder.to_schema())

    elif profiles_dir is not None and profile_name is not None:
        credentials, profile_data = extract_credentials_and_data_from_profiles(profiles_dir, profile_name)
        credentials_type = credentials.type
        if credentials_type == 'snowflake':
            snowflake_con = snowflake.connector.connect(
                account=credentials.account,
                user=credentials.user,
                database=credentials.database,
                schema=credentials.schema,
                warehouse=credentials.warehouse,
                role=credentials.role,
                autocommit=True,
                client_session_keep_alive=credentials.client_session_keep_alive,
                application='elementary',
                **credentials.auth_args()
            )

            with snowflake_con.cursor() as cursor:
                cursor.execute("""
                                    SELECT IDENTIFIER(:1) 
                                    FROM IDENTIFIER(:2) 
                                    WHERE IDENTIFIER(:3) >= :4;
                                """
                               , (json_column_name,
                                  table_name,
                                  time_column,
                                  start_time))

                rows = cursor.fetchall()
                for row in tqdm(rows, desc=f"Extracting and parsing jsons from {table_name}", colour='green'):
                    builder.add_object(json.loads(row[0]))

        write_schema_json_to_output_files(builder.to_schema())

    if os.path.exists("my_schema.yml"):
        with open("my_schema.yml", 'r') as schema_yml_file:
            open('my_schema.json', 'w').write(json.dumps(yaml.load(schema_yml_file)))

    config = GenerationConfiguration(copy_css=True, expand_buttons=True, with_footer=False, template_name='js',
                                     show_toc=False)
    generate_from_filename("my_schema.json", "schema_doc.html", config=config)
    webbrowser.open('schema_doc.html')


if __name__ == "__main__":
    elementary_data_reliability()

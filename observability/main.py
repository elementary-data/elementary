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
import os
import subprocess
from ruamel.yaml import YAML
import glob
from os.path import expanduser

snowflake.connector.paramstyle = 'numeric'

f = Figlet(font='slant')
print(f.renderText('Elementary'))

ELEMENTARY_DBT_PACKAGE_NAME = 'elementary_observability'
ELEMENTARY_DBT_PACKAGE = 'git@github.com:elementary-data/elementary-dbt.git'
ELEMENTARY_DBT_PACKAGE_VERSION = 'master'

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style
yaml.preserve_quotes = True
# import yaml
# import yamlloader


# TODO: move this to a view in our dbt pakcage
SELECT_UNSTRUCTURED_COLUMNS_FROM_INFORMATION_SCHEMA = """
                        SELECT column_name 
                            FROM information_schema.columns 
                            WHERE data_type in ('VARIANT') and collate(table_name, 'en-ci') = :2 and 
                            collate(table_schema, 'en-ci') = :1;
                        """
# TODO: move this to a view in our dbt pakcage
SELECT_UNSTRUCTURED_DATA = """
                            SELECT IDENTIFIER(:1)
                            FROM IDENTIFIER(:2)
                            WHERE IDENTIFIER(:3) >= :4;
                        """


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

    print("Adding elementary data reliability package to your dbt packages.")
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

    print("Running 'dbt deps' to download the package (this might take a while).")
    dbt_deps_result = subprocess.run(["dbt", "deps"], check=False, capture_output=True)
    if dbt_deps_result.returncode != 0:
        print(dbt_deps_result.stdout.decode('utf-8'))
        return

    print("Adding elementary data reliability package to your 'dbt_project.yml'.")
    with open('dbt_project.yml', 'r') as dbt_project_file:
        dbt_project = yaml.load(dbt_project_file)

    elementary_dbt_package_models = {ELEMENTARY_DBT_PACKAGE_NAME: {'+enabled': False}}
    if 'models' in dbt_project and dbt_project['models'] is not None:
        dbt_project['models'].update(elementary_dbt_package_models)
    else:
        dbt_project['models'] = elementary_dbt_package_models

    with open('dbt_project.yml', 'w') as dbt_project_file:
        yaml.dump(dbt_project, dbt_project_file)

    print("Running 'dbt deps' to download elementary data reliability package dependencies (this might take a while).")
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
    '--start-time', '-s',
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]),
    default=str(date.today() - timedelta(days=2))
)
def profile_sources(ctx, start_time):
    click.echo(f"Debug is {'on' if ctx.obj['DEBUG'] else 'off'}")

    if not os.path.exists('dbt_project.yml'):
        raise ConfigError('Please run this command from your main dbt project')

    with open('dbt_project.yml', 'r') as dbt_project_file:
        #dbt_project = yaml.load(dbt_project_file, Loader=yamlloader.ordereddict.CSafeLoader)
        dbt_project = yaml.load(dbt_project_file)

    #TODO: make this a dbt utility
    profile_name = dbt_project['profile']
    profiles_dir = os.path.join(expanduser("~"), '.dbt')
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
    else:
        #TODO: use specific exception
        raise ConfigError(f'Unsupported platform {credentials_type}')

    schema_file_paths = glob.glob(os.path.join('models', '*.yml'))
    for schema_file_path in schema_file_paths:
        with open(str(schema_file_path), 'r') as schema_file:
            #model_schema = yaml.load(schema_file, Loader=yamlloader.ordereddict.CSafeLoader)
            model_schema = yaml.load(schema_file)
            if 'sources' in model_schema:
                sources = model_schema['sources']

                for source in sources:
                    builder = SchemaBuilder()
                    #TODO: make sure the name of the source is the schema?
                    source_name = source.get('name')
                    source_db = source.get('database', credentials.database)
                    source_loaded_at_field = source.get('loaded_at_field')
                    source_tables = source.get('tables')

                    if source_name is None or source_loaded_at_field is None or source_tables is None:
                        continue

                    for source_table in source_tables:
                        source_table_name = source_table.get('name')

                        with snowflake_con.cursor() as cursor:
                            cursor.execute('use database IDENTIFIER(:1)', (source_db,))
                            cursor.execute(SELECT_UNSTRUCTURED_COLUMNS_FROM_INFORMATION_SCHEMA, (source_name,
                                                                                                 source_table_name))

                            rows = cursor.fetchall()
                            for row in rows:
                                unstructured_column = row[0]
                                cursor.execute(SELECT_UNSTRUCTURED_DATA, (unstructured_column,
                                                                          '.'.join([source_db,
                                                                                    source_name,
                                                                                    source_table_name]),
                                                                          source_loaded_at_field,
                                                                          start_time))
                                rows = cursor.fetchall()
                                for row in tqdm(rows, desc=f"Extracting and parsing unstructured data "
                                                           f"from {source_name}.{source_table_name}", colour='green'):
                                    builder.add_object(json.loads(row[0]))

                    # TODO: Maybe should be under the column itself
                    source['meta'] = {'json_schema': builder.to_schema()}

        with open(str(schema_file_path), 'w') as schema_file:
            #yaml.dump(model_schema, schema_file, width=100, Dumper=yamlloader.ordereddict.CDumper)
            yaml.dump(model_schema, schema_file)


if __name__ == "__main__":
    elementary_data_reliability()

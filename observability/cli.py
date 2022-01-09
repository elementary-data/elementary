import click
from datetime import date, timedelta
import snowflake.connector

from exceptions.exceptions import ConfigError
from lineage.dbt_utils import extract_credentials_and_data_from_profiles
from genson import SchemaBuilder
import json
from tqdm import tqdm
from json_schema_for_humans.generate import generate_from_filename
from json_schema_for_humans.generation_configuration import GenerationConfiguration
import webbrowser
from datetime import datetime
import os
import subprocess
import glob
from os.path import expanduser
import csv
from pathlib import Path


from observability.alerts import SchemaChangeUnstructuredDataAlert
from utils.yaml import get_ordered_yaml

snowflake.connector.paramstyle = 'numeric'


ELEMENTARY_DBT_PACKAGE_NAME = 'elementary_observability'
ELEMENTARY_DBT_PACKAGE = 'git@github.com:elementary-data/elementary-dbt.git'
ELEMENTARY_DBT_PACKAGE_VERSION = 'json_schemas'

yaml = get_ordered_yaml()


# TODO: move this to a view in our dbt pakcage
SELECT_COLUMNS_FROM_INFORMATION_SCHEMA = """
                        SELECT column_name, data_type 
                            FROM information_schema.columns 
                            WHERE collate(table_name, 'en-ci') = :2 and 
                            collate(table_schema, 'en-ci') = :1;
                        """
# TODO: move this to a view in our dbt pakcage
SELECT_UNSTRUCTURED_DATA = """
                            SELECT IDENTIFIER(:1)
                            FROM IDENTIFIER(:2)
                            WHERE IDENTIFIER(:3) >= :4;
                        """

# TODO: support pulling it from a dedicated schema for elementary observability
# TODO: should be in the dbt package
SELECT_SCHEMA_CHANGES_ALERTS = """
    SELECT full_table_name, total_jsons, bad_jsons, bad_jsons_rate, min_timestamp, max_timestamp, validation_details
    FROM JSON_SCHEMA_VALIDATION_RESULTS
    WHERE bad_jsons_rate > 50;
"""

MONITORING_CONFIGURATION_EXISTS = """
    SELECT count(*) from IDENTIFIER(:1);
"""



@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def observability(ctx, debug):
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug


@observability.command()
@click.pass_context
def init(ctx):
    if not os.path.exists('dbt_project.yml'):
        raise ConfigError("Please run this command from a dbt project directory")

    #TODO: replace with dbt hub name and latest version once we deploy to dbt hub

    elementary_dbt_package = {'git': ELEMENTARY_DBT_PACKAGE,
                              'revision': ELEMENTARY_DBT_PACKAGE_VERSION}

    print("Adding elementary data reliability package to your dbt packages.")
    dbt_packages = {'packages': []}
    if os.path.exists('packages.yml'):
        with open('packages.yml', 'r') as packages_file:
            dbt_packages = yaml.load(packages_file)

    #TODO: replace revision with version once deployed to dbt hub
    found = False
    for dbt_package in dbt_packages['packages']:
        if dbt_package.get('package') == ELEMENTARY_DBT_PACKAGE or dbt_package.get('git') == ELEMENTARY_DBT_PACKAGE:
            dbt_package['revision'] = ELEMENTARY_DBT_PACKAGE_VERSION
            found = True

    if not found:
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


@observability.command()
@click.pass_context
@click.option(
    '--start-time', '-s',
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]),
    default=str(date.today() - timedelta(days=7))
)
@click.option(
    '--html', '-h',
    type=bool,
    default=True,
)
@click.option(
    '--update-sources', '-u',
    type=bool,
    default=True,
)
@click.option(
    '--selected-source', '-f',
    type=str,
    default=None
)
def profile_sources(ctx, start_time: datetime, html: bool, update_sources: bool, selected_source: str):
    if not os.path.exists('dbt_project.yml'):
        raise ConfigError('Please run this command from your main dbt project')

    with open('dbt_project.yml', 'r') as dbt_project_file:
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

    config = GenerationConfiguration(copy_css=True, expand_buttons=True, template_name='js', show_toc=False)

    schema_file_paths = glob.glob(os.path.join('models', '*.yml'))
    for schema_file_path in schema_file_paths:
        with open(str(schema_file_path), 'r') as schema_file:
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

                    if selected_source is not None and selected_source != source_name:
                        continue

                    for source_table in source_tables:
                        source_table_name = source_table.get('name')
                        if 'columns' in source_table:
                            source_table_columns = source_table['columns']
                        else:
                            source_table_columns = source_table['columns'] = []

                        with snowflake_con.cursor() as cursor:
                            cursor.execute('use database IDENTIFIER(:1)', (source_db,))
                            cursor.execute(SELECT_COLUMNS_FROM_INFORMATION_SCHEMA, (source_name,
                                                                                    source_table_name))

                            results = cursor.fetchall()
                            for result in results:
                                column_name = result[0]
                                column_type = result[1]

                                # TODO: find a more elegant way to do that
                                found = False
                                for source_table_column in source_table_columns:
                                    if source_table_column.get('name') == column_name:
                                        source_table_column['meta'] = {'type': column_type}
                                        found = True
                                        break

                                if not found:
                                    source_table_column = {'name': column_name, 'meta': {'type': column_type}}
                                    source_table_columns.append(source_table_column)

                                if column_type.lower() in {'variant'}:
                                    cursor.execute(SELECT_UNSTRUCTURED_DATA, (column_name,
                                                                              '.'.join([source_db,
                                                                                        source_name,
                                                                                        source_table_name]),
                                                                              source_loaded_at_field,
                                                                              start_time))
                                    rows = cursor.fetchall()
                                    for row in tqdm(rows, desc=f"Extracting and parsing unstructured data "
                                                               f"from {source_name}.{source_table_name}", colour='green'):
                                        builder.add_object(json.loads(row[0]))

                                    unstructured_column_json_schema = builder.to_schema()
                                    source_table_column['meta'].update({'json_schema': unstructured_column_json_schema})
                                    if html:
                                        # TODO: create these under temp or a folder that gets cleaned
                                        json_file_name = f"{source_table_name}.json"
                                        with open(json_file_name, 'w') as json_file:
                                            json_file.write(json.dumps(unstructured_column_json_schema))
                                        html_file_name = f"{source_table_name}.html"
                                        generate_from_filename(json_file_name, html_file_name, config=config)
                                        webbrowser.open(html_file_name)

        if update_sources:
            with open(str(schema_file_path), 'w') as schema_file:
                yaml.dump(model_schema, schema_file)


def sources_to_csv(dbt_project_path: str, profile_database: str, csv_file_name: str):
    schema_file_paths = glob.glob(os.path.join(dbt_project_path, 'models', '*.yml'))
    with open(csv_file_name, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['full_table_name', 'column_name', 'type', 'json_schema',
                                                     'is_loaded_at_field', 'monitored'])
        writer.writeheader()
        for schema_file_path in schema_file_paths:
            with open(str(schema_file_path), 'r') as schema_file:
                model_schema = yaml.load(schema_file)
                if 'sources' in model_schema:
                    sources = model_schema['sources']
                    for source in sources:
                        source_name = source.get('name')
                        source_db = source.get('database', profile_database)
                        source_loaded_at_field = source.get('loaded_at_field')
                        source_tables = source.get('tables', [])

                        for source_table in source_tables:
                            source_table_name = source_table.get('name')
                            full_table_name = '.'.join([source_db, source_name, source_table_name])
                            source_table_columns = source_table.get('columns', [])
                            source_table_metadata = source_table.get('meta', {})
                            source_table_monitored = source_table_metadata.get('monitored', True)

                            for source_table_column in source_table_columns:
                                column_name = source_table_column.get('name')
                                column_metadata = source_table_column.get('meta', {})
                                column_type = column_metadata.get('type')
                                column_json_schema = column_metadata.get('json_schema')
                                if column_json_schema is not None:
                                    column_json_schema = json.dumps(column_json_schema)
                                column_monitored = column_metadata.get('monitored', source_table_monitored)
                                is_loaded_at_field = column_name == source_loaded_at_field
                                writer.writerow({'full_table_name': full_table_name,
                                                 'column_name': column_name,
                                                 'type': column_type,
                                                 'json_schema': column_json_schema,
                                                 'is_loaded_at_field': is_loaded_at_field,
                                                 'monitored': column_monitored})


@observability.command()
@click.pass_context
@click.option(
    '--update-config', '-u',
    type=bool,
    default=False,
)
def run(ctx, update_config):

    if not os.path.exists('dbt_project.yml'):
        raise ConfigError('Please run this command from your main dbt project')

    dbt_project_path = os.getcwd()
    with open('dbt_project.yml', 'r') as dbt_project_file:
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

    os.chdir(os.path.join('dbt_packages', ELEMENTARY_DBT_PACKAGE_NAME))

    monitoring_configuration_exists = False
    with snowflake_con.cursor() as cursor:
        try:
            #TODO: query this only if it exists in the information schema, move to the dbt package
            cursor.execute(MONITORING_CONFIGURATION_EXISTS, ('.'.join([credentials.database, credentials.schema,
                                                                       'monitoring_configuration']),))
            results = cursor.fetchall()
            for result in results:
                if result[0] > 0:
                    monitoring_configuration_exists = True
        except snowflake.connector.errors.ProgrammingError:
            pass

    if not monitoring_configuration_exists or update_config:
        Path('seeds').mkdir(parents=True, exist_ok=True)
        configuration_path = os.path.join('seeds', 'monitoring_configuration.csv')
        sources_to_csv(dbt_project_path, credentials.database, configuration_path)
        dbt_seed_result = subprocess.run(["dbt", "seed"], check=False, capture_output=True)
        if dbt_seed_result.returncode != 0:
            print(dbt_seed_result.stdout.decode('utf-8'))
            return

    print("Running elementary observability dbt package (this might take a while).")
    dbt_snapshot_result = subprocess.run(["dbt", "snapshot"], check=False, capture_output=True)
    if dbt_snapshot_result.returncode != 0:
        print(dbt_snapshot_result.stdout.decode('utf-8'))
        return

    if ctx.obj['DEBUG']:
        print(dbt_snapshot_result.stdout)

    dbt_run_result = subprocess.run(["dbt", "run", "-m", ELEMENTARY_DBT_PACKAGE_NAME], check=False, capture_output=True)
    if dbt_run_result.returncode != 0:
        print(dbt_run_result.stdout.decode('utf-8'))
        return

    print("Elementary observability package was run successfully.")
    if ctx.obj['DEBUG']:
        print(dbt_run_result.stdout)

    with open(os.path.join(expanduser("~"), '.edr', 'config.yml'), 'r') as edr_config_file:
        edr_config = yaml.load(edr_config_file)

    elementary_webhook = edr_config.get('notifications', {}).get('webhook')
    if elementary_webhook is not None:
        with snowflake_con.cursor() as cursor:
            cursor.execute(SELECT_SCHEMA_CHANGES_ALERTS)
            rows = cursor.fetchall()

            for row in rows:
                alert = SchemaChangeUnstructuredDataAlert(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
                alert.send_to_slack(elementary_webhook)


if __name__ == "__main__":
    observability()

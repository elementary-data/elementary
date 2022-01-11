import click
from datetime import date, timedelta
import snowflake.connector
from observability.config import Config

from exceptions.exceptions import ConfigError
from observability.data_monitoring import DataMonitoring
from utils.dbt import extract_credentials_and_data_from_profiles
from genson import SchemaBuilder
import json
from tqdm import tqdm
from json_schema_for_humans.generate import generate_from_filename
from json_schema_for_humans.generation_configuration import GenerationConfiguration
import webbrowser
from datetime import datetime
import os
import glob
from os.path import expanduser
from utils.ordered_yaml import OrderedYaml

snowflake.connector.paramstyle = 'numeric'


ELEMENTARY_DBT_PACKAGE_NAME = 'elementary_observability'
ELEMENTARY_DBT_PACKAGE = 'git@github.com:elementary-data/elementary-dbt.git'
ELEMENTARY_DBT_PACKAGE_VERSION = 'json_schemas'

yaml = OrderedYaml()


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def observability(ctx, debug):
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug


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


@observability.command()
@click.pass_context
@click.option(
    '--config-dir-path', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr')
)
@click.option(
    '--profiles-dir-path', '-p',
    type=str,
    default=os.path.join(expanduser('~'), '.dbt')
)
@click.option(
    '--force-update-dbt-package', '-f',
    type=bool,
    default=False
)
@click.option(
    '--reload-monitoring-configuration', '-r',
    type=bool,
    default=False
)
def run(ctx, config_dir_path, profiles_dir_path, force_update_dbt_package, reload_monitoring_configuration):
    config = Config(config_dir_path, profiles_dir_path)
    data_monitoring = DataMonitoring.create_data_monitoring(config)
    data_monitoring.run(force_update_dbt_package, reload_monitoring_configuration)


if __name__ == "__main__":
    observability()

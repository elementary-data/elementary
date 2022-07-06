from setuptools import setup, find_packages
import pathlib

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name='elementary-data',
    description='Data monitoring and lineage',
    version='0.4.2',
    packages=find_packages(),
    python_requires='>=3.6.2',
    entry_points='''
        [console_scripts]
        edr=cli.cli:cli
    ''',
    author="Elementary",
    package_data={"": ["header.html", "index.html", "dbt_project/*", "dbt_project/macros/*", "dbt_project/models/*"]},
    keyword="data, lineage, data lineage, data warehouse, DWH, observability, data monitoring, data observability, "
            "Snowflake, BigQuery, Redshift, data reliability, analytics engineering",
    long_description=README,
    install_requires=[
        'click>=7.0,<9',
        'pyfiglet',
        'dbt-core>=0.20,<2.0.0',
        'requests<3.0.0',
        'beautifulsoup4',
        'posthog',
        'ruamel.yaml',
        'alive-progress<=2.3.1',
        'slack-sdk<4.0',
        'pydantic<2.0'
    ],
    extras_require={
        'snowflake': ['dbt-snowflake>=0.20,<2.0.0'],
        'bigquery': ['dbt-bigquery>=0.20,<2.0.0'],
        'redshift': ['dbt-redshift>=0.20,<2.0.0'],
        'postgres': ['dbt-postgres>=0.20,<2.0.0'],
        'lineage': ['snowflake-connector-python[secure-local-storage]>=2.4.1,<2.8.0',
                    'click>=8,<9',
                    'requests>=2.7,<3.0.0',
                    'networkx>=2.3,<3',
                    'sqllineage==1.2.4',
                    'pyvis>=0.1,<1',
                    'sqlparse>=0.3.1,<0.5',
                    'sqlfluff<0.9.0',
                    'google-cloud-bigquery>=1.25.0,<3',
                    'google-api-core>=1.16.0,<3',
                    'python-dateutil>=2.7.2,<3.0.0',
                    # temp solution until refactor of utils.dbt
                    'dbt-bigquery>=0.20,<2.0.0',
                    # Indirect
                    'protobuf<4,>=3.13.0',
                    'typing-extensions<3.11,>=3.7.4',
                    'flask<1.1.1',
                    'Jinja2>=2.10.1,<3',
                    'tomli<2.0.0,>=0.2.6',
                    'urllib3<1.26,>=1.20',
                    'MarkupSafe==2.0.1']
    },
    long_description_content_type="text/markdown",
    license='',
    url='https://github.com/elementary-data/elementary',
    author_email='or@elementary-data.com',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
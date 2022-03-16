from setuptools import setup, find_packages
import pathlib

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name='elementary-data',
    description='Data monitoring and lineage',
    version='0.2.2',
    packages=find_packages(),
    python_requires='>=3.6.2',
    entry_points='''
        [console_scripts]
        edr=cli.cli:cli
    ''',
    author="Elementary",
    package_data={"": ["header.html", "dbt_project/*", "dbt_project/macros/*", "dbt_project/models/*"]},
    keyword="data, lineage, data lineage, data warehouse, DWH, observability, data monitoring, data observability, "
            "Snowflake, BigQuery, Redshift, data reliability, analytics engineering",
    long_description=README,
    install_requires=[
        'click>=8,<9',
        'pyfiglet',
        'snowflake-connector-python[secure-local-storage]>=2.4.1,<2.8.0',
        'dbt-core>=0.20,<1.1.0',
        'dbt-postgres>=0.20,<1.1.0',
        'dbt-redshift>=0.20,<1.1.0',
        'dbt-snowflake>=0.20,<1.1.0',
        'dbt-bigquery>=0.20,<1.1.0',
        'networkx>=2.3,<3',
        'sqllineage==1.2.4',
        'pyvis>=0.1,<1',
        'sqlparse>=0.3.1,<0.5',
        'alive-progress',
        'python-dateutil>=2.7.2,<3.0.0',
        'google-cloud-bigquery>=1.25.0,<3',
        'google-api-core>=1.16.0,<3',
        'requests>=2.7,<3.0.0',
        'beautifulsoup4',
        'posthog',
        'sqlfluff<0.9.0',
        'ruamel.yaml',
        # Indirect
        'protobuf<4,>=3.13.0',
        'typing-extensions<3.11,>=3.7.4',
        'flask<1.1.1',
        'Jinja2>=2.10.1,<3',
        'tomli<2.0.0,>=0.2.6',
        'urllib3<1.26,>=1.20'
    ],
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
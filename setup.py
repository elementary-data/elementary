import pathlib

from setuptools import setup, find_packages

# The directory containing this file.
HERE = pathlib.Path(__file__).parent

# The text of the README file.
README = (HERE / "README.md").read_text()

setup(
    name="elementary-data",
    description="Data monitoring and lineage",
    version="0.5.4",
    packages=find_packages(),
    python_requires=">=3.6.2",
    entry_points="""
        [console_scripts]
        edr=elementary.cli.cli:cli
    """,
    author="Elementary",
    package_data={
        "": [
            "header.html",
            "index.html",
            "dbt_project/*",
            "dbt_project/macros/*",
            "dbt_project/models/*",
            "dbt_project/models/alerts/*",
        ]
    },
    keyword="data, lineage, data lineage, data warehouse, DWH, observability, data monitoring, data observability, "
    "Snowflake, BigQuery, Redshift, data reliability, analytics engineering",
    long_description=README,
    install_requires=[
        "click>=7.0,<9",
        "pyfiglet",
        "dbt-core>=0.20,<2.0.0",
        "requests<3.0.0",
        "beautifulsoup4<5.0.0",
        "posthog<3.0.0",
        "boto3<2.0.0",
        "google-cloud-storage<3.0.0",
        "ruamel.yaml<1.0.0",
        "alive-progress<=2.3.1",
        "slack-sdk>=3.19.2,<4.0.0",
        "pydantic<2.0",
        "networkx>=2.3,<3",
        "packaging>=20.9,<22.0",
    ],
    extras_require={
        "snowflake": ["dbt-snowflake>=0.20,<2.0.0"],
        "bigquery": ["dbt-bigquery>=0.20,<2.0.0"],
        "redshift": ["dbt-redshift>=0.20,<2.0.0"],
        "postgres": ["dbt-postgres>=0.20,<2.0.0"],
        "databricks": ["dbt-databricks>=0.20,<2.0.0"],
        "spark": ["dbt-spark>=0.20,<2.0.0", "dbt-spark[PyHive]>=0.20,<2.0.0"],
    },
    long_description_content_type="text/markdown",
    license="",
    url="https://github.com/elementary-data/elementary",
    author_email="or@elementary-data.com",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)

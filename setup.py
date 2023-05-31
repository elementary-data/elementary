import pathlib

from setuptools import find_packages, setup

# The directory containing this file.
HERE = pathlib.Path(__file__).parent

# The text of the README file.
README = (HERE / "README.md").read_text(encoding="UTF-8")

ADAPTER_EXTRA_REQUIREMENTS = {
    "snowflake": ["dbt-snowflake>=0.20,<2.0.0"],
    "bigquery": ["dbt-bigquery>=0.20,<2.0.0"],
    "redshift": ["dbt-redshift>=0.20,<2.0.0"],
    "postgres": ["dbt-postgres>=0.20,<2.0.0"],
    "databricks": ["dbt-databricks>=0.20,<2.0.0"],
    "spark": ["dbt-spark>=0.20,<2.0.0", "dbt-spark[PyHive]>=0.20,<2.0.0"],
}


setup(
    name="elementary-data",
    description="Data monitoring and lineage",
    version="0.8.0",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.6.2",
    entry_points="""
        [console_scripts]
        edr=elementary.cli.cli:cli
    """,
    author="Elementary",
    keyword="data, lineage, data lineage, data warehouse, DWH, observability, data monitoring, data observability, "
    "Snowflake, BigQuery, Redshift, data reliability, analytics engineering",
    long_description=README,
    install_requires=[
        "click>=7.0,<9",
        "pyfiglet",
        "dbt-core>=0.20,<2.0.0",
        "requests>=2.28.1,<3.0.0",
        "beautifulsoup4<5.0.0",
        "ratelimit",
        "posthog<3.0.0",
        "boto3<2.0.0",
        "google-cloud-storage<3.0.0",
        "ruamel.yaml<1.0.0",
        "alive-progress<=2.3.1",
        "slack-sdk>=3.20.1,<4.0.0",
        "pytest-parametrization>=2022.2.1",
        "pydantic<2.0",
        "networkx>=2.3,<3",
        "packaging>=20.9,<23.0",
    ],
    extras_require={
        **ADAPTER_EXTRA_REQUIREMENTS,
        "all": sum(ADAPTER_EXTRA_REQUIREMENTS.values(), []),
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

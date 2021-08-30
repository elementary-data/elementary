# Elementary Data Lineage
Data lineage based on your data warehourse query history
# Installation
## Using Pip
```bash
  $ pip install elementary-lineage
```
## Manual
```bash
  $ git clone https://github.com/oravi/lineage.git
  $ cd lineage
  $ python setup.py install
```
# Usage
```bash
$ lineage --help
```
## Using a configuration file
```bash
$ lineage --start-date 2021-08-29 --end-date 2021-08-30 -c ~/lineage_configuration.yml
```
## Using dbt profiles.yml

```bash
$ lineage --start-date 2021-08-29 --end-date 2021-08-30 --dbt-profiles-dir ~/.dbt --dbt-profile-name my_snowflake_profile
```
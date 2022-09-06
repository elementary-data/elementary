### Overview
1. Prerequisites
1. Configure credentials
1. Setup Postgres (optional)
1. Setup virtual environment
1. Installation for development
1. Run the integration tests
1. Run tests
1. Creating a new integration test

### Prerequisites
- python3
- Docker

### Configure credentials
Edit the env file for your TARGET in `integration_tests/.env/[TARGET].env`.

Load the environment variables:
```shell
set -a; source integration_tests/.env/[TARGET].env; set +a
```

or more specific:
```shell
set -a; source integration_tests/.env/postgres.env; set +a
```

#### Setup Postgres (optional)

Docker and `docker-compose` are both used in testing. Specific instructions for your OS can be found [here](https://docs.docker.com/get-docker/).

Postgres offers the easiest way to test most `dbt-utils` functionality today. Its tests are the fastest to run, and the easiest to set up. To run the Postgres integration tests, you'll have to do one extra step of setting up the test database:

```shell
make setup-db
```
or, alternatively:
```shell
docker-compose up --detach postgres
```

### Setup virtual environment

We strongly recommend using virtual environments when developing code in `dbt-utils`. We recommend creating this virtualenv
in the root of the `dbt-utils` repository. To create a new virtualenv, run:
```shell
python3 -m venv env
source env/bin/activate
```

This will create and activate a new Python virtual environment.

### Installation for development

First make sure that you set up your virtual environment as described above.  Also ensure you have the latest version of pip installed with `pip install --upgrade pip`. Next, install `dbt-core` (and its dependencies) with:

```shell
make dev target=[postgres|redshift|...]
# or
pip install --pre dbt-[postgres|redshift|...] -r dev-requirements.txt
```

or more specific:

```shell
make dev target=postgres
# or
pip install --pre dbt-postgres -r dev-requirements.txt
```

### Run the integration tests

To run all the integration tests on your local machine like they will get run in the CI (using CircleCI):

```shell
make test target=postgres
```

or, to run tests for a single model:
```shell
make test target=[postgres|redshift|...] [models=...] [seeds=...]
```

or more specific:

```shell
make test target=postgres models=sql.test_star seeds=sql.data_star
```

Specying `models=` and `seeds=` is optional, however _if_ you specify `seeds`, you have to specify `models` too.

Where possible, targets are being run in docker containers (this works for Postgres or in the future Spark for example). For managed services like Snowflake, BigQuery and Redshift this is not possible, hence your own configuration for these services has to be provided in the appropriate env files in `integration_tests/.env/[TARGET].env`

### Creating a new integration test

#### Set up profiles
Do either one of the following:
1. Use `DBT_PROFILES_DIR`
    ```shell
    cp integration_tests/ci/sample.profiles.yml integration_tests/profiles.yml
    export DBT_PROFILES_DIR=$(cd integration_tests && pwd)
    ```
2. Use `~/.dbt/profiles.yml`
    - Copy contents from `integration_tests/ci/sample.profiles.yml` into `~/.dbt/profiles.yml`.

#### Add your integration test
This directory contains an example dbt project which tests the macros in the `dbt-utils` package. An integration test typically involves making 1) a new seed file 2) a new model file 3) a generic test to assert anticipated behaviour.

For an example integration tests, check out the tests for the `get_url_parameter` macro:

1. [Macro definition](https://github.com/fishtown-analytics/dbt-utils/blob/master/macros/web/get_url_parameter.sql)
2. [Seed file with fake data](https://github.com/fishtown-analytics/dbt-utils/blob/master/integration_tests/data/web/data_urls.csv)
3. [Model to test the macro](https://github.com/fishtown-analytics/dbt-utils/blob/master/integration_tests/models/web/test_urls.sql)
4. [A generic test to assert the macro works as expected](https://github.com/fishtown-analytics/dbt-utils/blob/master/integration_tests/models/web/schema.yml#L2)

Once you've added all of these files, you should be able to run:

Assuming you are in the `integration_tests` folder,
```shell
dbt deps --target {your_target}
dbt seed --target {your_target}
dbt run --target {your_target} --model {your_model_name}
dbt test --target {your_target} --model {your_model_name}
```

Alternatively:
```shell
dbt deps --target {your_target}
dbt build --target {your_target} --select +{your_model_name}
```

If the tests all pass, then you're good to go! All tests will be run automatically when you create a PR against this repo.
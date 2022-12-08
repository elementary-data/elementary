FROM python:3.8

COPY . .
RUN pip install ".[snowflake, bigquery, redshift, databricks]"

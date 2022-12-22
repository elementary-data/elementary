FROM python:3.10.7

WORKDIR /app
RUN pip install "elementary-data[snowflake, bigquery, redshift, databricks]"
ENTRYPOINT ["edr"]

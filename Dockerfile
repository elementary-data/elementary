FROM python:3.8

WORKDIR /app
RUN pip install "elementary-data[snowflake, bigquery, redshift, databricks]"
ENTRYPOINT ["edr"]

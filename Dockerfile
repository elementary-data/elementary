FROM python:3.9

WORKDIR /app
RUN pip install "elementary-data[snowflake, bigquery, redshift, databricks]"
ENTRYPOINT ["edr"]

FROM python:3.10.7

WORKDIR /app
COPY . .
RUN pip install ".[snowflake, bigquery, redshift, databricks]"
ENTRYPOINT ["edr"]


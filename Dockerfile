FROM python:3.10.7

ARG USR_APP_PATH=/usr/app
ENV DBT_LOG_PATH=$USR_APP_PATH/logs
ENV DBT_TARGET_PATH=$USR_APP_PATH/target
WORKDIR $USR_APP_PATH
RUN chmod 777 .

COPY . /app
RUN pip install --no-cache-dir "/app[postgres, snowflake, bigquery, redshift, databricks]"

ENTRYPOINT ["edr"]

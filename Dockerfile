FROM python:3.10.7

ARG USR_APP_PATH=/usr/app
ENV DBT_LOG_PATH=$USR_APP_PATH/logs
ENV DBT_TARGET_PATH=$USR_APP_PATH/target
WORKDIR $USR_APP_PATH
RUN chmod 777 .

RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
    python-dev \
    libsasl2-dev \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

COPY . /app
RUN pip install --upgrade pip
RUN pip install --no-cache-dir "/app[all]"

ENTRYPOINT ["edr"]
Those features will allow Elementary to get all required info for computing the data lineage graph.

### Connecting Looker to Elementary

Navigate to the **Account settings > Environments** and choose the environment to which you would like to connect Elementary.
Choose the Power BI connection and provide the following details to validate and complete the integration.
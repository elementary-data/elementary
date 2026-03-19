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
ARG UV_VERSION=0.10.11
RUN pip install --no-cache-dir "uv==${UV_VERSION}" \
    && uv pip install --no-cache --system "/app[all]"

ENTRYPOINT ["edr"]

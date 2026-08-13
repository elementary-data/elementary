FROM python:3.12

ARG USR_APP_PATH=/usr/app
ENV DBT_LOG_PATH=$USR_APP_PATH/logs
ENV DBT_TARGET_PATH=$USR_APP_PATH/target
WORKDIR $USR_APP_PATH
RUN chmod 777 .

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    python3-dev \
    libsasl2-dev \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

COPY . /app
ARG UV_VERSION=0.12.3
# --directory is what makes uv read /app/pyproject.toml, so the security floors in
# [tool.uv] constraint-dependencies apply to the image as well.
RUN pip install --no-cache-dir "uv==${UV_VERSION}" \
    && uv pip install --no-cache --system --directory /app ".[all]"

ENTRYPOINT ["edr"]

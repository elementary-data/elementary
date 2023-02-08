FROM python:3.10.7

ARG USERNAME=elementary
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

USER $USERNAME

WORKDIR /app
COPY --chown=$USER_UID:$USER_GID . .
RUN pip install  --user ".[postgres, snowflake, bigquery, redshift, databricks]"
ENTRYPOINT ["edr"]


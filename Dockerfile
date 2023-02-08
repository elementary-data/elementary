FROM python:3.10.7

ARG USERNAME=elementary
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

USER $USERNAME
WORKDIR /usr/app

COPY --chown=$USER_UID:$USER_GID . /app
RUN pip install --user "/app[postgres, snowflake, bigquery, redshift, databricks]"
ENV PATH="$PATH:/home/$USERNAME/.local/bin"

ENTRYPOINT ["edr"]

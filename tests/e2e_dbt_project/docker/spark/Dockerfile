# syntax=docker/dockerfile:1

ARG SPARK_IMAGE=apache/spark:3.3.3
FROM ${SPARK_IMAGE}

# Delta 3.0 renamed delta-core to delta-spark, so the artifact name (with its
# Scala suffix) is passed in alongside the version.
ARG DELTA_ARTIFACT=delta-core_2.12
ARG DELTA_VERSION=2.2.0
ARG POSTGRES_JDBC_VERSION=42.7.13

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# ADD gives files downloaded from a URL mode 600 (root only), hence a+r.
ADD --chmod=a+r \
    "https://repo1.maven.org/maven2/io/delta/${DELTA_ARTIFACT}/${DELTA_VERSION}/${DELTA_ARTIFACT}-${DELTA_VERSION}.jar" \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    ${SPARK_HOME}/jars/

# Spark's default warehouse dir is relative to the working directory; keep it
# at /spark-warehouse, where docker-compose mounts its volume.
WORKDIR /

ENTRYPOINT ["spark-submit"]
CMD ["--help"]

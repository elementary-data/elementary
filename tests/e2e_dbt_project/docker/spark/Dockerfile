# The official image ships the Spark distribution, so we don't download the
# ~300MB tarball from archive.apache.org, which is heavily throttled and used
# to take 40-70 minutes in CI.
ARG SPARK_IMAGE=apache/spark:v3.3.2

FROM ${SPARK_IMAGE} AS spark

# The Spark image has no wget/curl, so fetch the extra jars from Maven Central
# in a separate stage.
FROM alpine:3.22 AS jars
ARG SCALA_BINARY_VERSION=2.12
ARG DELTA_VERSION=2.2.0
ARG POSTGRES_JDBC_VERSION=42.7.13
RUN --mount=type=bind,from=spark,source=/opt/spark/jars,target=/spark-jars <<'EOF'
set -eu
# Delta 3.0 renamed the delta-core artifact to delta-spark.
delta_artifact="delta-spark_${SCALA_BINARY_VERSION}"
if [ "${DELTA_VERSION%%.*}" -lt 3 ]; then
  delta_artifact="delta-core_${SCALA_BINARY_VERSION}"
fi
jars="io/delta/${delta_artifact}/${DELTA_VERSION}/${delta_artifact}-${DELTA_VERSION}.jar
io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar
org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"
if [ "${DELTA_VERSION%%.*}" -ge 4 ]; then
  # Delta 4 also depends on Delta Kernel. Spark already ships Kernel's other
  # dependencies (Hadoop, Parquet, Jackson, RoaringBitmap) except
  # jackson-datatype-jdk8, which we match to Spark's Jackson version.
  jackson_version=$(ls /spark-jars | sed -n 's/^jackson-databind-\(.*\)\.jar$/\1/p')
  for artifact in delta-kernel-api delta-kernel-defaults delta-kernel-unitycatalog; do
    jars="$jars
io/delta/${artifact}/${DELTA_VERSION}/${artifact}-${DELTA_VERSION}.jar"
  done
  jars="$jars
com/fasterxml/jackson/datatype/jackson-datatype-jdk8/${jackson_version}/jackson-datatype-jdk8-${jackson_version}.jar"
fi
mkdir /jars
for jar in $jars; do
  wget -q -P /jars "https://repo1.maven.org/maven2/${jar}"
done
EOF

FROM spark

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

COPY --from=jars --chmod=644 /jars/ ${SPARK_HOME}/jars/
COPY --chmod=755 entrypoint.sh /scripts/entrypoint.sh

# Spark's default warehouse dir is relative to the working directory; keep it
# at /spark-warehouse, where docker-compose mounts its volume.
WORKDIR /

ENTRYPOINT ["/scripts/entrypoint.sh"]
CMD ["--help"]

ARG OPENJDK_VERSION=8
FROM eclipse-temurin:${OPENJDK_VERSION}-jre

ARG SPARK_VERSION=3.3.2
ARG HADOOP_VERSION=3
ARG DELTA_VERSION=2.2.0

ENV SPARK_HOME /usr/spark
ENV PATH="/usr/spark/bin:/usr/spark/sbin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget netcat-openbsd procps libpostgresql-jdbc-java && \
    wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /usr/spark && \
    ln -s /usr/share/java/postgresql-jdbc4.jar /usr/spark/jars/postgresql-jdbc4.jar && \
    wget -q "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar" \
         -P /usr/spark/jars/ && \
    wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
         -P /usr/spark/jars/ && \
    apt-get remove -y wget && \
    apt-get autoremove -y && \
    apt-get clean

COPY entrypoint.sh /scripts/
RUN chmod +x /scripts/entrypoint.sh

ENTRYPOINT ["/scripts/entrypoint.sh"]
CMD ["--help"]

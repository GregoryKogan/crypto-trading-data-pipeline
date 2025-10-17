FROM apache/spark:3.5.1

USER root

# Fix for GPG key errors:
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates gnupg2 python3-pip wget && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean


ENV SPARK_VERSION=3.5.1
ENV KAFKA_CLIENTS_VERSION=3.7.0
ENV POSTGRES_JDBC_VERSION=42.7.3

# Download JARs for Kafka and PostgreSQL connectivity
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar && \
    wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar && \
    wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar

RUN mkdir -p /opt/spark/app

COPY requirements.txt /opt/spark/app/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/app/requirements.txt

RUN chown -R spark:spark /opt/spark/app

USER spark

WORKDIR /opt/spark/app
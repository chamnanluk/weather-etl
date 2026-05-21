FROM apache/airflow:2.9.3-python3.11

USER root

ARG SPARK_VERSION=3.5.5
ARG HADOOP_PROFILE=hadoop3
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG AIRFLOW_CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
    && curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
      | tar -xz -C /opt \
    && ln -s "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" /opt/spark \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-trino \
    boto3 \
    pandas \
    trino

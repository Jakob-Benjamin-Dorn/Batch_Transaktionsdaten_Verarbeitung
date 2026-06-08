FROM apache/airflow:2.10.4-python3.11

ARG AIRFLOW_VERSION=2.10.4

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH=/opt/airflow

COPY requirements.txt /opt/airflow/requirements.txt

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /opt/airflow/requirements.txt

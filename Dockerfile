ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev \
 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY . /opt/airflow/project
ENV PYTHONPATH=/opt/airflow/project

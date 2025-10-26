FROM apache/airflow:2.7.2-python3.10

USER root

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Crear directorios
RUN mkdir -p /opt/airflow/logs \
    /opt/airflow/dags \
    /opt/airflow/plugins \
    /opt/airflow/sql \
    /opt/airflow/src

# Establecer permisos
RUN chown -R airflow:root /opt/airflow

USER airflow

# Copiar e instalar dependencias Python
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

WORKDIR /opt/airflow

ENV PYTHONPATH=/opt/airflow
ENV AIRFLOW_HOME=/opt/airflow
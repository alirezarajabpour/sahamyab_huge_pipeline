#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command -v python) -m pip install --upgrade pip
  $(command -v pip) install -r requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

$(command -v airflow) db upgrade

airflow webserver &
airflow scheduler
#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_VERSION="${AIRFLOW_VERSION:-2.9.3}"
PYVER="$(python -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')"

echo "[*] Upgrading pip..."
python -m pip install --upgrade pip

echo "[*] Installing Airflow with constraints ${AIRFLOW_VERSION} (Python ${PYVER})..."
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYVER}.txt"

echo "[*] Installing providers and libs..."
pip install apache-airflow-providers-postgres pandas kaggle psycopg2-binary python-dotenv

echo "[*] Initializing Airflow DB..."
export AIRFLOW_HOME="${AIRFLOW_HOME:-$(pwd)}"
airflow db init

echo "[*] Creating admin user..."
airflow users create \
  --username admin --password admin \
  --firstname Atharv --lastname Jagtap \
  --role Admin --email admin@example.com

echo "[*] Creating Postgres connection 'pg_netflix'..."
airflow connections add pg_netflix \
  --conn-uri 'postgresql+psycopg2://postgres:postgres@localhost:5432/airflow_db'

echo "[*] Done. Now run 'airflow webserver -p 8080' and 'airflow scheduler'."

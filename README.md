# Netflix ELT Pipeline with Airflow + PostgreSQL + Kaggle

A clean, reproducible ELT pipeline that:
- **Extracts** the public *Netflix Movies and TV Shows* dataset from Kaggle
- **Loads** it into PostgreSQL with idempotent, fast inserts
- **Transforms** into a clean table for analysis
- **Runs** on a daily schedule with Airflow
- **Ships** with logs, scripts and a GitHub‑ready structure

---

## 🧱 Project Structure

```
airflow/
 ├── dags/
 │   └── netflix_pipeline_dag.py
 ├── data/
 │   └── netflix_titles.csv
 ├── scripts/
 │   ├── setup_airflow.sh
 │   └── env.example
 ├── requirements.txt
 └── README.md
```

> You can place this folder anywhere (e.g., `~/airflow_home`). Make sure your Airflow `dags_folder` points to `./dags` or add this path to Airflow config.

---

## ⚙️ Step 1: Environment Setup

### Option A — Local (WSL/Ubuntu/Kali)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
```
![1](assets/1.png)

```bash
# Airflow requires constraints; use the current stable (example below, update if needed)
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION=$(python -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
pip install "apache-airflow==${AIRFLOW_VERSION}"   --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Providers
pip install apache-airflow-providers-postgres pandas kaggle psycopg2-binary python-dotenv
```
![2](assets/2.png)

### Option B — Docker for PostgreSQL only (Airflow local)

If you don't have Postgres locally, run a temp container:

```bash
docker run -d --name pg-netflix -e POSTGRES_PASSWORD=postgres   -e POSTGRES_DB=airflow_db -p 5432:5432 postgres:16
```

---

## 🔑 Step 2: Kaggle API

1) On Kaggle: **Account → Create New API Token** (downloads `kaggle.json`)  
2) Place it at `~/.kaggle/kaggle.json`  
3) Restrict permissions:

```bash
chmod 600 ~/.kaggle/kaggle.json
```
![3](assets/3.png)

> The DAG uses the `kaggle` CLI under the hood.

---

## 🗄️ Step 3: PostgreSQL Connection in Airflow

Create an Airflow connection named **`pg_netflix`**:  
**UI:** *Admin → Connections → +*  
- Conn Id: `pg_netflix`  
- Conn Type: `Postgres`  
- Host: `localhost`  
- Schema: `airflow_db`  
- Login: `postgres`  
- Password: `postgres` (or your own)  
- Port: `5432`

**CLI alternative:**

```bash
airflow connections add pg_netflix   --conn-uri 'postgresql+psycopg2://postgres:postgres@localhost:5432/airflow_db'
```
![4](assets/4.png)

---

## 🪄 Step 4: Initialize Airflow and User

```bash
export AIRFLOW_HOME=$(pwd)
airflow db init

airflow users create   --username admin --password admin   --firstname Turbo --lastname Ath   --role Admin --email turboath@example.com
```
![5](assets/5.png)

---

## 🧠 Step 5: Enable and Run the Pipeline

Start the services (two terminals):

```bash
# Terminal 1
airflow webserver -p 8080

# Terminal 2
airflow scheduler
```
![6](assets/6.png)

Open: http://localhost:8080 → trigger **`netflix_elt_pipeline`**.

---

## 🧪 Step 6: Validate Data

Connect to Postgres and run:

```sql
SELECT COUNT(*) FROM netflix;
SELECT COUNT(*) FROM netflix_clean;

SELECT country, COUNT(*) AS c
FROM netflix_clean
GROUP BY country
ORDER BY c DESC
LIMIT 10;
```

---

## 📝 Logging

- Each Airflow task logs to the task instance logs.
- The DAG also logs structured messages (start/end, row counts, durations).
- You can review logs from the Airflow UI → *Graph* → click any task → *Log*.

---

## 🧰 Scripts

- `scripts/setup_airflow.sh` — optional helper that installs deps, initializes Airflow and creates the connection.
- `scripts/env.example` — sample environment variables to customize.

---

## 📦 Requirements

```
apache-airflow
apache-airflow-providers-postgres
pandas
psycopg2-binary
kaggle
python-dotenv
```

> Airflow must be installed with the matching constraints file (see Step 1).

---

## 🧾 License

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)  
Released under the [MIT License](LICENSE) © 2025 Atharv Yadav

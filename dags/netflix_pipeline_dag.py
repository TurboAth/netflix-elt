from __future__ import annotations

import os
import io
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---------- Config ----------
DATA_DIR = os.getenv("DATA_DIR", "/tmp/netflix_data")
KAGGLE_DATASET = "shivamb/netflix-shows"  # public dataset
RAW_CSV = os.path.join(DATA_DIR, "netflix_titles.csv")
PG_CONN_ID = os.getenv("PG_CONN_ID", "pg_netflix")


default_args = {
    "owner": "atharv",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="netflix_elt_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 11, 7),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "elt", "kaggle", "postgres"],
)
def netflix_elt_pipeline():
    log = logging.getLogger("airflow")

    @task
    def extract() -> str:
        """
        Download & unzip the dataset from Kaggle CLI into DATA_DIR.
        """
        os.makedirs(DATA_DIR, exist_ok=True)
        log.info("Starting extract: dataset=%s, dest=%s", KAGGLE_DATASET, DATA_DIR)

        # Use kaggle CLI; it reads ~/.kaggle/kaggle.json
        exit_code = os.system(
            f'kaggle datasets download -d {KAGGLE_DATASET} -p "{DATA_DIR}" --unzip'
        )
        if exit_code != 0:
            raise AirflowFailException("Kaggle download failed. Check your API key and network.")

        if not os.path.exists(RAW_CSV):
            # In case the file was named differently, try to locate it
            candidates = [p for p in os.listdir(DATA_DIR) if p.endswith(".csv")]
            if candidates:
                src = os.path.join(DATA_DIR, candidates[0])
                os.rename(src, RAW_CSV)

        if not os.path.exists(RAW_CSV):
            raise AirflowFailException("CSV not found after download/unzip.")

        # Quick sanity read (no memory blowup)
        head = pd.read_csv(RAW_CSV, nrows=5)
        log.info("Extract success. Sample rows:\n%s", head.to_string(index=False))

        return RAW_CSV

    @task
    def load(csv_path: str) -> int:
        """
        Create target table and load rows using a staging table + fast COPY,
        then upsert into the target table on (show_id).
        Returns number of rows staged.
        """
        log.info("Starting load from %s", csv_path)
        df = pd.read_csv(csv_path)

        # Normalize column names to expected schema
        expected_cols = [
            "show_id", "type", "title", "director", "cast", "country",
            "date_added", "release_year", "rating", "duration",
            "listed_in", "description"
        ]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            raise AirflowFailException(f"Missing columns in CSV: {missing}")

        # Create staging-friendly dataframe (ensure strings, strip)
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].fillna("").astype(str).str.strip()

        # Connect
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        create_target_sql = """
        CREATE TABLE IF NOT EXISTS netflix (
            show_id TEXT PRIMARY KEY,
            type TEXT,
            title TEXT,
            director TEXT,
            cast TEXT,
            country TEXT,
            date_added TEXT,
            release_year INT,
            rating TEXT,
            duration TEXT,
            listed_in TEXT,
            description TEXT
        );
        """
        create_staging_sql = """
        CREATE TEMP TABLE IF NOT EXISTS netflix_staging (
            show_id TEXT,
            type TEXT,
            title TEXT,
            director TEXT,
            cast TEXT,
            country TEXT,
            date_added TEXT,
            release_year INT,
            rating TEXT,
            duration TEXT,
            listed_in TEXT,
            description TEXT
        ) ON COMMIT DROP;
        """

        with engine.begin() as conn:
            conn.execute(text(create_target_sql))  # type: ignore
            conn.execute(text(create_staging_sql))  # type: ignore

        # COPY into staging
        from sqlalchemy import text
        csv_buf = io.StringIO()
        # Ensure consistent ordering of columns
        df.to_csv(csv_buf, index=False, header=False, columns=expected_cols)
        csv_buf.seek(0)

        # Use raw connection for copy_expert
        pg_conn = hook.get_conn()
        try:
            with pg_conn, pg_conn.cursor() as cur:
                cur.copy_expert(
                    f"COPY netflix_staging ({', '.join(expected_cols)}) FROM STDIN WITH CSV",
                    csv_buf,
                )
        finally:
            pg_conn.close()

        staged_rows = len(df)
        log.info("Staged %d rows.", staged_rows)

        # Upsert into target
        upsert_sql = """
        INSERT INTO netflix AS t (
            show_id, type, title, director, cast, country,
            date_added, release_year, rating, duration, listed_in, description
        )
        SELECT
            show_id, type, title, director, cast, country,
            date_added, release_year, rating, duration, listed_in, description
        FROM netflix_staging
        ON CONFLICT (show_id) DO UPDATE SET
            type = EXCLUDED.type,
            title = EXCLUDED.title,
            director = EXCLUDED.director,
            cast = EXCLUDED.cast,
            country = EXCLUDED.country,
            date_added = EXCLUDED.date_added,
            release_year = EXCLUDED.release_year,
            rating = EXCLUDED.rating,
            duration = EXCLUDED.duration,
            listed_in = EXCLUDED.listed_in,
            description = EXCLUDED.description;
        """

        with engine.begin() as conn:
            conn.execute(text(upsert_sql))

        log.info("Load completed (upserted).")
        return staged_rows

    @task
    def transform() -> int:
        """
        Create/refresh a clean table with basic quality filters.
        Returns number of rows in clean table.
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        from sqlalchemy import text
        with hook.get_sqlalchemy_engine().begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS netflix_clean AS
                SELECT * FROM netflix WHERE 1=0;
            """))
            conn.execute(text("TRUNCATE TABLE netflix_clean;"))
            conn.execute(text("""
                INSERT INTO netflix_clean
                SELECT DISTINCT ON (show_id) *
                FROM netflix
                WHERE COALESCE(country, '') <> '';
            """))
            result = conn.execute(text("SELECT COUNT(*) FROM netflix_clean;"))
            (cnt,) = list(result)[0]
        logging.getLogger("airflow").info("Transform produced %d rows.", cnt)
        return int(cnt)

    # Orchestration
    csv_path = extract()
    _ = load(csv_path)
    _ = transform()


dag = netflix_elt_pipeline()

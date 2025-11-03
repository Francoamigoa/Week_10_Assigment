from __future__ import annotations
import os
import csv
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Error as DatabaseError

from faker import Faker

OUTPUT_DIR = "/opt/airflow/data"
SCHEMA = "week8_demo"
TARGET_TABLE = "employees"
CONN_ID = "Postgres"

default_args = {
    "owner": "ids706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


with DAG(
    dag_id="Homework_week10",
    description="Homework_week10",
    start_date=datetime(2025, 10, 1),
    schedule="0 22 * * *",  # diario 10 pm
    catchup=False,
    default_args=default_args,
) as dag:

    @task()
    def init_workspace(base_dir: str = OUTPUT_DIR) -> dict:
        ensure_dir(base_dir)
        tmp = os.path.join(base_dir, "tmp_etl")
        ensure_dir(tmp)
        return {"base": base_dir, "tmp": tmp}

    with TaskGroup(group_id="ingest_transform") as ingest_transform:

        @task()
        def generate_people(tmp_dir: str) -> str:
            fake = Faker()
            path = os.path.join(tmp_dir, "people.csv")
            cols = [
                "person_id",
                "first_name",
                "last_name",
                "email",
                "company_id",
                "state",
            ]
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(cols)
                for pid in range(1, 201):
                    first = fake.first_name()
                    last = fake.last_name()
                    email = f"{first.lower()}.{last.lower()}@example.com"
                    company_id = fake.random_int(min=1, max=50)
                    state = fake.state_abbr()
                    w.writerow([pid, first, last, email, company_id, state])
            return path  # XCom: solo path

        @task()
        def generate_companies(tmp_dir: str) -> str:
            fake = Faker()
            path = os.path.join(tmp_dir, "companies.csv")
            cols = ["company_id", "company_name", "domain"]
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(cols)
                for cid in range(1, 51):
                    name = fake.company()
                    domain = name.replace(" ", "").replace(",", "").lower() + ".com"
                    w.writerow([cid, name, domain])
            return path

        @task()
        def transform_people(in_path: str, tmp_dir: str) -> str:
            out_path = os.path.join(tmp_dir, "people_tidy.csv")
            with open(in_path, encoding="utf-8") as f, open(
                out_path, "w", newline="", encoding="utf-8"
            ) as g:
                r = csv.DictReader(f)
                cols = ["person_id", "full_name", "email", "company_id", "state"]
                w = csv.writer(g)
                w.writerow(cols)
                for row in r:
                    full_name = f"{row['first_name']} {row['last_name']}".strip()
                    email = row["email"].strip().lower() or None
                    w.writerow(
                        [
                            row["person_id"],
                            full_name,
                            email,
                            row["company_id"],
                            row["state"],
                        ]
                    )
            return out_path

        @task()
        def transform_companies(in_path: str, tmp_dir: str) -> str:
            out_path = os.path.join(tmp_dir, "companies_tidy.csv")
            with open(in_path, encoding="utf-8") as f, open(
                out_path, "w", newline="", encoding="utf-8"
            ) as g:
                r = csv.DictReader(f)
                cols = ["company_id", "company_name", "domain"]
                w = csv.writer(g)
                w.writerow(cols)
                for row in r:
                    dom = row["domain"].lower()
                    w.writerow([row["company_id"], row["company_name"], dom])
            return out_path

    @task()
    def merge_csvs(people_csv: str, companies_csv: str, base_dir: str) -> str:
        out_path = os.path.join(base_dir, "employees_final.csv")
        # Cargar companies a dict
        companies = {}
        with open(companies_csv, encoding="utf-8") as fc:
            rc = csv.DictReader(fc)
            for r in rc:
                companies[r["company_id"]] = {
                    "company_name": r["company_name"],
                    "domain": r["domain"],
                }

        with open(people_csv, encoding="utf-8") as fp, open(
            out_path, "w", newline="", encoding="utf-8"
        ) as fo:
            rp = csv.DictReader(fp)
            cols = [
                "person_id",
                "full_name",
                "email",
                "company_id",
                "state",
                "company_name",
                "domain",
            ]
            w = csv.writer(fo)
            w.writerow(cols)
            for r in rp:
                c = companies.get(r["company_id"])
                if c:
                    w.writerow(
                        [
                            r["person_id"],
                            r["full_name"],
                            r["email"],
                            r["company_id"],
                            r["state"],
                            c["company_name"],
                            c["domain"],
                        ]
                    )
        return out_path

    @task()
    def load_csv_to_pg(
        conn_id: str,
        csv_path: str,
        schema: str = SCHEMA,
        table: str = TARGET_TABLE,
        append: bool = True,
    ) -> int:
        # Leer CSV
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = [
                tuple((r.get(col, "") or None) for col in fieldnames) for r in reader
            ]

        if not rows:
            print("CSV sin filas. Nada que insertar.")
            return 0

        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        create_table = f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({', '.join([f'{c} TEXT' for c in fieldnames])});"
        delete_rows = f"DELETE FROM {schema}.{table};" if not append else None
        insert_sql = f"INSERT INTO {schema}.{table} ({', '.join(fieldnames)}) VALUES ({', '.join(['%s']*len(fieldnames))});"

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)
                if delete_rows:
                    cur.execute(delete_rows)
                cur.executemany(insert_sql, rows)
                conn.commit()
            print(f"Insertadas {len(rows)} filas en {schema}.{table}")
            return len(rows)
        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @task()
    def analyze_from_db(
        conn_id: str,
        schema: str = SCHEMA,
        table: str = TARGET_TABLE,
        base_dir: str = OUTPUT_DIR,
    ) -> dict:
        """
        Lee desde Postgres y genera:
        - un resumen por estado a CSV
        - opcionalmente un gráfico si matplotlib está disponible
        """
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = f"""
            SELECT state, COUNT(*) AS n
            FROM {schema}.{table}
            GROUP BY state
            ORDER BY n DESC;
        """
        out_csv = os.path.join(base_dir, "summary_by_state.csv")
        rows = []
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        # Exportar CSV
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["state", "n"])
            w.writerows(rows)

        # Intentar generar gráfico sin romper si no hay matplotlib
        plot_path = os.path.join(base_dir, "summary_by_state.png")
        try:
            import matplotlib.pyplot as plt

            states = [r[0] for r in rows]
            counts = [int(r[1]) for r in rows]
            plt.figure(figsize=(14, 6))
            plt.bar(states, counts)
            plt.title("Employees by state")
            plt.xlabel("State")
            plt.ylabel("Count")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            plt.savefig(plot_path, dpi=150)
            plt.close()
            made_plot = True
        except Exception as e:
            print(f"No se generó gráfico: {e}")
            made_plot = False

        return {"csv": out_csv, "plot": plot_path if made_plot else None}

    @task()
    def cleanup(tmp_dir: str):
        if os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)
        return "ok"

    # Orquestación
    ws = init_workspace()
    with ingest_transform:
        p_raw = generate_people(ws["tmp"])
        c_raw = generate_companies(ws["tmp"])
        p_tidy = transform_people(p_raw, ws["tmp"])
        c_tidy = transform_companies(c_raw, ws["tmp"])

        # Paralelismo dentro del TaskGroup:
        [p_raw, c_raw]  # se ejecutan a la vez
        [p_tidy, c_tidy]  # se ejecutan a la vez, cada una depende de su raw

    merged = merge_csvs(p_tidy, c_tidy, ws["base"])
    loaded = load_csv_to_pg(CONN_ID, merged, SCHEMA, TARGET_TABLE, append=True)
    analyzed = analyze_from_db(CONN_ID, SCHEMA, TARGET_TABLE, ws["base"])
    done = cleanup(ws["tmp"])

    # Dependencias globales
    ws >> ingest_transform >> merged >> loaded >> analyzed >> done

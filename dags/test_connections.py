from datetime import datetime

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


@dag(
    dag_id="test_connections",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
)
def test_connections():
    @task
    def check_mysql():
        hook = MySqlHook(mysql_conn_id="mysql_staging")
        version = hook.get_first("SELECT VERSION()")[0]
        print(f"Connected to MySQL staging. Version: {version}")

    @task
    def check_postgres():
        hook = PostgresHook(postgres_conn_id="postgres_analytics")
        version = hook.get_first("SELECT version()")[0]
        print(f"Connected to Postgres analytics. Version: {version}")

    check_mysql()
    check_postgres()


test_connections()

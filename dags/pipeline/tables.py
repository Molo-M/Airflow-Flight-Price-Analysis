"""
Creates the database tables by running the CREATE TABLE statements in the sql/ folder.
"""

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pipeline.config import MYSQL_CONN_ID, MYSQL_TABLES_SQL, POSTGRES_CONN_ID, POSTGRES_TABLES_SQL


def run_sql_file(hook, path):
    """Read a .sql file and run every statement in it, one after another."""
    with open(path) as file:
        sql = file.read()
    hook.run(sql, split_statements=True)
    print(f"Ran {path}")


def create_mysql_tables():
    """
    Create the MySQL staging tables by running sql/mysql_tables.sql.
    Safe to run repeatedly, because each statement uses CREATE TABLE IF NOT EXISTS.
    """
    run_sql_file(MySqlHook(mysql_conn_id=MYSQL_CONN_ID), MYSQL_TABLES_SQL)


def create_postgres_tables():
    """
    Create the PostgreSQL analytics tables by running sql/postgres_tables.sql.
    Safe to run repeatedly, because each statement uses CREATE TABLE IF NOT EXISTS.
    """
    run_sql_file(PostgresHook(postgres_conn_id=POSTGRES_CONN_ID), POSTGRES_TABLES_SQL)
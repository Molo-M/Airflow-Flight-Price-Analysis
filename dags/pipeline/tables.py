"""
Creates the database tables by running the CREATE TABLE statements in the sql/ folder.
"""

from airflow.providers.mysql.hooks.mysql import MySqlHook

from pipeline.config import MYSQL_CONN_ID, MYSQL_TABLES_SQL


def create_mysql_tables():
    """
    Create the MySQL tables by running every statement in sql/mysql_tables.sql.
    Safe to run repeatedly, because each statement uses CREATE TABLE IF NOT EXISTS.
    """
    with open(MYSQL_TABLES_SQL) as file:
        sql = file.read()

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    # split_statements=True runs each statement in the file one after another
    hook.run(sql, split_statements=True)
    print(f"MySQL tables created (or already existed) from {MYSQL_TABLES_SQL}")
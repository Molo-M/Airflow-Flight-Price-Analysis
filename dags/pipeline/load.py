"""
Stage 4: Loading into PostgreSQL.

Copies the enriched flight data and the KPI tables from MySQL to the
PostgreSQL analytics database.

All tables are replaced inside ONE transaction: either every table gets the
new data, or (if anything fails) none of them change. While the load runs,
anyone querying PostgreSQL keeps seeing the previous data until the commit.
"""

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pipeline.config import (
    BOOLEAN_COLUMNS,
    ENRICHED_TABLE,
    LOAD_TABLES,
    MYSQL_CONN_ID,
    POSTGRES_BATCH_SIZE,
    POSTGRES_CONN_ID,
)
from pipeline.db import dataframe_to_rows, read_query


def insert_in_batches(cursor, table, df, batch_size):
    """
    Insert a DataFrame's rows into a table, `batch_size` rows per INSERT statement.
    One statement per batch is much faster than one statement per row.
    """
    columns = list(df.columns)
    column_list = ", ".join(columns)
    one_row_placeholder = "(" + ", ".join(["%s"] * len(columns)) + ")"  # e.g. (%s, %s, %s)
    rows = dataframe_to_rows(df)

    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]

        # One placeholder group per row: VALUES (%s, %s), (%s, %s), ...
        placeholders = ", ".join([one_row_placeholder] * len(batch))

        # All the batch's values in one flat list, in the same order as the placeholders
        values = []
        for row in batch:
            values.extend(row)

        cursor.execute(f"INSERT INTO {table} ({column_list}) VALUES {placeholders}", values)


def load_to_postgres():
    """
    Copy every table in LOAD_TABLES from MySQL to PostgreSQL in one transaction.
    Each table's row count is checked before committing; if any check or insert
    fails, everything is rolled back and PostgreSQL keeps its previous data.

    Returns the number of rows loaded into each table.
    """
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # 1. Read every table from MySQL first, so the PostgreSQL transaction stays short
    tables = {}
    for table in LOAD_TABLES:
        df = read_query(mysql_hook, f"SELECT * FROM {table}")

        # MySQL stores the flags as 1/0; PostgreSQL BOOLEAN columns need True/False
        for column in BOOLEAN_COLUMNS:
            if column in df.columns:
                df[column] = df[column].astype(bool)

        tables[table] = df
        print(f"Read {len(df)} rows from MySQL table {table}")

    if tables[ENRICHED_TABLE].empty:
        raise ValueError(f"{ENRICHED_TABLE} is empty in MySQL. Did the earlier steps run?")

    # 2. Replace every PostgreSQL table inside a single transaction
    connection = postgres_hook.get_conn()
    try:
        cursor = connection.cursor()

        for table, df in tables.items():
            cursor.execute(f"DELETE FROM {table}")  # unlike TRUNCATE, doesn't block readers
            insert_in_batches(cursor, table, df, POSTGRES_BATCH_SIZE)

            # Check inside the transaction, so a bad load can still be rolled back
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            loaded_count = cursor.fetchone()[0]
            if loaded_count != len(df):
                raise ValueError(f"{table}: loaded {loaded_count} rows, expected {len(df)}")
            print(f"Loaded {loaded_count} rows into PostgreSQL table {table}")

        connection.commit()  # all tables switch to the new data at the same moment
        print("Committed: all tables loaded into PostgreSQL")

    except Exception:
        connection.rollback()  # undo everything; PostgreSQL keeps its previous data
        print("Load failed: rolled back, PostgreSQL data is unchanged")
        raise  # re-raise the original error so the task fails (and the alert fires)

    finally:
        connection.close()

    return {table: int(len(df)) for table, df in tables.items()}

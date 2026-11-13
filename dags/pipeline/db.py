"""
Small database helpers shared by the pipeline stages.

read_query             runs a SELECT and returns the result as a pandas DataFrame
dataframe_to_rows      turns a DataFrame into rows of plain Python values for inserting
replace_table_contents empties a MySQL table and fills it with a DataFrame's rows
"""

import pandas as pd


def read_query(hook, sql):
    """
    Run a SELECT statement and return the result as a pandas DataFrame.
    The column names come from the query itself.
    """
    connection = hook.get_conn()
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        column_names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
    finally:
        connection.close()  # always close the connection, even if the query fails

    return pd.DataFrame(list(rows), columns=column_names)


def dataframe_to_rows(df):
    """
    Turn a DataFrame into a list of tuples, one per row, ready for inserting.
    Database drivers need Python None for missing values, not pandas' NaN/NaT,
    and plain Python values rather than pandas/numpy ones.
    """
    clean_df = df.astype(object).where(df.notna(), None)
    return list(clean_df.itertuples(index=False, name=None))


def replace_table_contents(hook, table, df, batch_size=5000):
    """
    Empty a MySQL table, then insert every row of the DataFrame into it.
    The DataFrame's column names must match the table's column names.
    Emptying first makes the step safe to re-run (no duplicate rows).
    """
    rows = dataframe_to_rows(df)

    hook.run(f"TRUNCATE TABLE {table}")
    if rows:
        hook.insert_rows(
            table=table,
            rows=rows,
            target_fields=list(df.columns),
            commit_every=batch_size,
            executemany=True,
        )
    print(f"Wrote {len(rows)} rows to {table}")
import os
import math
import glob
import pandas as pd
import pyodbc

SERVER_NAME = "host.docker.internal,1433"
DATABASE_NAME = "RetailDW"
SQL_USER = "sa"
SQL_PASSWORD = "cruze100"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "output")

FILES_TO_LOAD = {
    "RetailAnalytics": os.path.join("retail_analytics", "*.csv"),
    "SalesByState": os.path.join("sales_by_state", "*.csv"),
    "SalesByCategory": os.path.join("sales_by_category", "*.csv")
}


def get_connection(database="master"):
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={database};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;",
        autocommit=True
    )


def create_database():
    conn = get_connection("master")
    cursor = conn.cursor()

    cursor.execute(f"""
    IF NOT EXISTS (
        SELECT name FROM sys.databases WHERE name = '{DATABASE_NAME}'
    )
    CREATE DATABASE {DATABASE_NAME}
    """)

    cursor.close()
    conn.close()
    print(f"Database ready: {DATABASE_NAME}")


def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    return value


def clean_dataframe(df):
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(clean_value)
    return df


def get_sql_type(series):
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(series):
        return "FLOAT"
    else:
        return "NVARCHAR(MAX)"


def create_table(cursor, table_name, df):
    cursor.execute(f"""
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
    DROP TABLE {table_name}
    """)

    columns_sql = []

    for col in df.columns:
        safe_col = col.replace(" ", "_").replace("-", "_")
        sql_type = get_sql_type(df[col])
        columns_sql.append(f"[{safe_col}] {sql_type}")

    create_sql = f"""
    CREATE TABLE {table_name} (
        {", ".join(columns_sql)}
    )
    """

    cursor.execute(create_sql)
    print(f"Table created: {table_name}")


def insert_data(cursor, table_name, df):
    safe_columns = [
        col.replace(" ", "_").replace("-", "_")
        for col in df.columns
    ]

    columns_sql = ", ".join([f"[{col}]" for col in safe_columns])
    placeholders = ", ".join(["?" for _ in safe_columns])

    insert_sql = f"""
    INSERT INTO {table_name} ({columns_sql})
    VALUES ({placeholders})
    """

    rows = [
        tuple(clean_value(value) for value in row)
        for row in df.itertuples(index=False, name=None)
    ]

    cursor.fast_executemany = False
    cursor.executemany(insert_sql, rows)

    print(f"Inserted {len(rows)} rows into {table_name}")


def get_spark_output_file(file_pattern):
    matched_files = glob.glob(os.path.join(OUTPUT_PATH, file_pattern))

    if not matched_files:
        raise FileNotFoundError(
            f"Missing file: {os.path.join(OUTPUT_PATH, file_pattern)}"
        )

    return matched_files[0]


def load_csv_to_sql(table_name, file_pattern):
    file_path = get_spark_output_file(file_pattern)

    print(f"Loading file: {file_path}")

    df = pd.read_csv(file_path)
    df = clean_dataframe(df)

    conn = get_connection(DATABASE_NAME)
    cursor = conn.cursor()

    create_table(cursor, table_name, df)
    insert_data(cursor, table_name, df)

    conn.commit()
    cursor.close()
    conn.close()


def main():
    create_database()

    for table_name, file_pattern in FILES_TO_LOAD.items():
        load_csv_to_sql(table_name, file_pattern)

    print("Load phase completed successfully.")
    print(f"All data loaded into SQL Server database: {DATABASE_NAME}")


if __name__ == "__main__":
    main()
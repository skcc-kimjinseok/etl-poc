from prefect import flow, task, get_run_logger
import dask.dataframe as dd
import cx_Oracle
import pandas as pd
import os
from sqlalchemy.dialects.oracle import FLOAT as OracleFLOAT
from sqlalchemy import create_engine, String, Float, DateTime


database_config = {
    "DB_HOST": os.environ.get("DB_HOST"),
    "DB_PORT": os.environ.get("DB_PORT"),
    "DB_USER": os.environ.get("DB_USER"),
    "DB_PASSWORD": os.environ.get("DB_PASSWORD"),
    "DB_NAME": os.environ.get("DB_NAME"),
}

storage_options = {
    "account_name": os.environ.get("AZ_ACCOUNT_NAME", ""),
    "account_key": os.environ.get("AZ_ACCOUNT_KEY", ""),
}

TARGET_TABLE = "TGTADM_AMEX_DATA"
ORACLE_DB_CONNECTION_STRING = f"oracle+cx_oracle://{database_config['DB_USER']}:{database_config['DB_PASSWORD']}@{database_config['DB_HOST']}:{database_config['DB_PORT']}/{database_config['DB_NAME']}"
engine = create_engine(ORACLE_DB_CONNECTION_STRING)

dsn = cx_Oracle.makedsn(
    f"{database_config['DB_HOST']}",
    int(database_config["DB_PORT"]),
    service_name=f"{database_config['DB_NAME']}",
)


@task(retries=3, retry_delay_seconds=10, log_prints=True)
def extract(file_path: str):
    logger = get_run_logger()
    try:
        logger.info(f"Reading Parquet file from Azure Blob Storage: {file_path}")
        full_path = f"abfs://{file_path}"
        dask_df = dd.read_parquet(full_path, storage_options=storage_options)
        logger.info(f"Read Parquet file successfully")
        return dask_df
    except Exception as e:
        logger.error(f"Error reading Parquet file: {e}")
        raise


def transform(data):
    logger = get_run_logger()
    try:
        df = data.compute()
        logger.info(f"transform data with {len(df)} rows")
        required_columns = ["customer_ID", "S_2", "P_2", "D_39", "D_136"]
        df = df[required_columns]
        df = df.rename(columns={"customer_ID": "CUSTOMER_ID"})
        df["S_2"] = pd.to_datetime(df["S_2"])
        df["P_2"] = df["P_2"].astype(float)
        df["D_39"] = df["D_39"].astype(float)
        df["D_136"] = df["D_136"].astype(float)
        logger.info("Data transformation completed.")
        return df
    except Exception as e:
        logger.error(f"Error transforming data chunk: {e}")
        raise


def bulk_insert_method(table, conn, keys, data_iter):
    connection = conn.connection
    cursor = connection.cursor()

    columns = ", ".join(keys)
    placeholders = ", ".join([f":{i+1}" for i in range(len(keys))])
    insert_sql = f"INSERT INTO {table.name} ({columns}) VALUES ({placeholders})"

    data = [tuple(row) for row in data_iter]

    cursor.executemany(insert_sql, data)
    connection.commit()


def load(df, table_name, chunk_size):
    column_types = {
        "CUSTOMER_ID": String(128),
        "S_2": DateTime,
        "P_2": Float(precision=53).with_variant(
            OracleFLOAT(binary_precision=126), "oracle"
        ),
        "D_39": Float(precision=53).with_variant(
            OracleFLOAT(binary_precision=126), "oracle"
        ),
        "D_136": Float(precision=53).with_variant(
            OracleFLOAT(binary_precision=126), "oracle"
        ),
    }
    logger = get_run_logger()
    try:
        logger.info(f"Loading data chunk to table {table_name}")
        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            chunksize=chunk_size,
            method=bulk_insert_method,
            dtype=column_types,
        )
    except Exception as e:
        logger.error(f"Error loading data chunk to database: {e}")
        raise


@task
def transform_and_load_chunk_data(chunk_data, target_table, chunk_size):
    transform_data = transform(chunk_data)
    load(transform_data, target_table, chunk_size)


@task
def check_db_table_exists_and_delete(table_name: str):
    connection_ = cx_Oracle.connect(
        user=f"{database_config['DB_USER']}",
        password=database_config["DB_PASSWORD"],
        dsn=dsn,
    )
    cursor = connection_.cursor()
    query = """
        SELECT table_name
        FROM all_tables
        WHERE table_name = :table_name
        """
    cursor.execute(query, [table_name.upper()])
    result = cursor.fetchone()
    if result:
        drop_table_query = f"DROP TABLE {table_name}"
        cursor.execute(drop_table_query)
    cursor.close()


@flow(
    name="etl-04-pipeline-dask",
)
def etl_04_flow(
    file_path: str = "etl-data/data.parquet",
    tgt_table: str = TARGET_TABLE,
    chunk_size: int = 5000,
):
    dask_df = extract(file_path)
    check_db_table_exists_and_delete(tgt_table)
    delayed_partitions = dask_df.to_delayed()
    for partition in delayed_partitions:
        transform_and_load_chunk_data(partition, tgt_table, chunk_size)


if __name__ == "__main__":
    etl_04_flow.serve(name="etl_04_pipeline")

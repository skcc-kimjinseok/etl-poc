import os
from prefect import task, flow, get_run_logger
import cx_Oracle
import pandas as pd
import dask.dataframe as dd
from sqlalchemy import create_engine, Table, MetaData, Column, CLOB
import os

# read config from env
database_config = {
    "DB_HOST": os.environ.get("DB_HOST"),
    "DB_PORT": os.environ.get("DB_PORT"),
    "DB_USER": os.environ.get("DB_USER"),
    "DB_PASSWORD": os.environ.get("DB_PASSWORD"),
    "DB_NAME": os.environ.get("DB_NAME"),
}

SOURCE_DB_URI = f"oracle+cx_oracle://{database_config['DB_USER']}:{database_config['DB_PASSWORD']}@{database_config['DB_HOST']}:{database_config['DB_PORT']}/{database_config['DB_NAME']}"
SOURCE_TABLE = "SRCADM_PROJECTS_DATA"
TARGET_TABLE = "TGTADM_PROJECTS_DATA"

engine = create_engine(SOURCE_DB_URI)


@task()
def drop_and_create_table(df, table_name):
    logger = get_run_logger()
    try:

        engine = create_engine(SOURCE_DB_URI)
        # Define metadata object to hold the schema
        metadata = MetaData()
        # Define the table schema using SQLAlchemy
        table = Table(table_name, metadata, *(Column(col, CLOB) for col in df.columns))
        # Connect to the database and reflect existing tables
        with engine.connect() as connection:
            # Drop the table if it exists
            if engine.dialect.has_table(connection, table_name):
                table.drop(engine)
                logger.info(f"Drop table {TARGET_TABLE} successfully")

            # Create the table if it doesn't exist
            if not engine.dialect.has_table(connection, table_name):
                metadata.create_all(engine)
                logger.info(f"Create table {TARGET_TABLE} successfully")
        # Dispose of the engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Error drop and recreate table {TARGET_TABLE}: {e}")
        raise


@task
def extract(chunk_size: int, source_table: str):
    logger = get_run_logger()
    try:
        query = f"SELECT * FROM {source_table}"
        df = pd.read_sql(query, con=engine)
        return dd.from_pandas(df, chunksize=chunk_size)
    except Exception as e:
        logger.error(f"Error extract data from table {source_table}: {e}")
        raise


@task
def transform(data):
    return data


# define bulk insert method
def bulk_insert_method(table, conn, keys, data_iter):
    connection = conn.connection
    cursor = connection.cursor()

    columns = ", ".join(keys)
    placeholders = ", ".join([f":{i+1}" for i in range(len(keys))])
    insert_sql = f"INSERT INTO {table.name} ({columns}) VALUES ({placeholders})"

    data = [tuple(row) for row in data_iter]

    cursor.executemany(insert_sql, data)
    connection.commit()


@task
def load(data, chunk_size, target_table):
    logger = get_run_logger()
    try:
        df = data.compute()
        df.to_sql(
            target_table,
            engine,
            if_exists="append",
            index=False,
            method=bulk_insert_method,
            chunksize=chunk_size,
        )
        logger.info(
            f"Save data with {len(df)} rows into table {target_table} successfully"
        )
    except Exception as e:
        logger.error(f"Error load data to target table {target_table}: {e}")
        raise


@flow(name="etl-06-pipeline")
def etl_06_flow(
    chunk_sise=5000, src_table=SOURCE_TABLE, tgt_table=TARGET_TABLE, parallel_load=False
):
    raw_data = extract(chunk_sise, src_table)
    transformed_data = transform(raw_data)
    drop_and_create_table(transformed_data, tgt_table)
    if parallel_load:
        partitions = transformed_data.to_delayed()
        load.map(partitions, chunk_sise, tgt_table)
    else:
        for part in transformed_data.to_delayed():
            load(part, chunk_sise, tgt_table)


if __name__ == "__main__":
    etl_06_flow.serve(name="etl-06-pipeline")

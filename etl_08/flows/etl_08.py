import os
from prefect import task, flow, get_run_logger
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, String, DateTime
import os
from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner

database_config = {
    "DB_HOST": os.environ.get("DB_HOST"),
    "DB_PORT": os.environ.get("DB_PORT"),
    "DB_USER": os.environ.get("DB_USER"),
    "DB_PASSWORD": os.environ.get("DB_PASSWORD"),
    "DB_NAME": os.environ.get("DB_NAME"),
}

SOURCE_DB_URI = f"oracle+cx_oracle://{database_config['DB_USER']}:{database_config['DB_PASSWORD']}@{database_config['DB_HOST']}:{database_config['DB_PORT']}/{database_config['DB_NAME']}"
SOURCE_TABLE = "SRCADM_PROJECTS_DATA"
TARGET_TABLE = "TGTADM_PROJECTS_DATA_TIMELINE_V2"
engine = create_engine(SOURCE_DB_URI)


@task()
def drop_table_if_exists(table_name):
    logger = get_run_logger()
    try:
        engine = create_engine(SOURCE_DB_URI)
        # Define metadata object to hold the schema
        metadata = MetaData()
        # Define the table schema using SQLAlchemy
        table = Table(table_name, metadata)
        # Connect to the database and reflect existing tables
        with engine.connect() as connection:
            # Drop the table if it exists
            if engine.dialect.has_table(connection, table_name):
                table.drop(engine)
                logger.info(f"Drop table {table_name} successfully")
        # Dispose of the engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Error drop table {table_name}: {e}")
        raise


@task()
def fetch_and_transform_data_in_chunks(table_name: str, chunk_size: int = 5000):
    query = f"SELECT * FROM {table_name}"
    chunk_iter = pd.read_sql(query, engine, chunksize=chunk_size)
    for chunk in chunk_iter:
        chunk["INSERT_TIME"] = chunk["boardapprovaldate"].dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        yield chunk


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
        df = data
        df.to_sql(
            target_table,
            engine,
            if_exists="append",
            index=False,
            method=bulk_insert_method,
            chunksize=chunk_size,
            dtype={"INSERT_TIME": String(20), "boardapprovaldate": DateTime},
        )
        logger.info(
            f"Save data with {len(df)} rows into table {target_table} successfully"
        )
    except Exception as e:
        logger.error(f"Error load data to target table {target_table}: {e}")
        raise


@task
def verify_data_row_count(src_table: str, tgt_table):
    source_query = f"SELECT COUNT(*) FROM {src_table}"
    target_query = f"SELECT COUNT(*) FROM {tgt_table}"

    source_count = pd.read_sql(source_query, engine).iloc[0, 0]
    target_count = pd.read_sql(target_query, engine).iloc[0, 0]

    return source_count == target_count


@task
def verify_data_integrity(src_table: str, tgt_table, sample_size=50):
    source_query = (
        f"SELECT * FROM {src_table} SAMPLE(1)"  # Adjust the sampling method as needed
    )
    source_sample = pd.read_sql(source_query, engine).sample(sample_size)
    for index, source_row in source_sample.iterrows():
        target_query = f"""SELECT * FROM {tgt_table} WHERE DBMS_LOB.COMPARE(id, '{source_row["id"]}') = 0"""
        target_df = pd.read_sql(target_query, engine)

        if target_df.empty:
            return False

    return True


@flow(task_runner=SequentialTaskRunner())
def load_chunks_sequentially(chunk_generator, chunk_size, tgt_table):
    load_tasks = []
    for chunk in chunk_generator:
        load_task = load(chunk, chunk_size, tgt_table)
        load_tasks.append(load_task)
    return load_tasks


@flow(task_runner=ConcurrentTaskRunner())
def verification_flow(src_table, tgt_table):
    logger = get_run_logger()
    row_count_check = verify_data_row_count.submit(src_table, tgt_table)
    data_integrity_check = verify_data_integrity.submit(src_table, tgt_table)

    if row_count_check.result() and data_integrity_check.result():
        logger.info("Data consistency verified successfully.")
    else:
        logger.error("Data consistency verification failed.")


@flow(name="etl-08-pipeline", task_runner=ConcurrentTaskRunner())
def etl_08_flow(chunk_sise=5000, src_table=SOURCE_TABLE, tgt_table=TARGET_TABLE):

    drop_table_if_exists(tgt_table)
    chunk_generator = fetch_and_transform_data_in_chunks(src_table)
    load_tasks = load_chunks_sequentially(chunk_generator, chunk_sise, tgt_table)
    verification_flow(src_table, tgt_table, wait_for=load_tasks)


if __name__ == "__main__":
    etl_08_flow.serve(name="etl-08-pipeline")

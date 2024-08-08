import pandas as pd
import cx_Oracle
from prefect import flow, task, serve, get_run_logger
from config import settings
from constant import Params
import dask.dataframe as dd
from sqlalchemy import create_engine, text, Table, MetaData, Column, String, CLOB
from dask import delayed
import dask
from concurrent.futures import ThreadPoolExecutor
import threading
from datetime import datetime
import time


host = settings.ORACLE_HOST
port = settings.ORACLE_PORT
sid = settings.ORACLE_SID
user = settings.ORACLE_USER
password = settings.ORACLE_PASSWORD

connection_string = f'oracle+cx_oracle://{user}:{password}@{host}:{port}/{sid}'
engine = create_engine(connection_string)

@task(log_prints=True)
def drop_and_create_table(df, table_name):
    try:
        metadata = MetaData()
        table = Table(
            table_name, metadata,
            *(Column(col, CLOB) for col in df.columns)
        )
        with engine.connect() as connection:
            # Drop the table if it exists
            if engine.dialect.has_table(connection, table_name):
                table.drop(engine)
            # Create the table if it doesn't exist
            if not engine.dialect.has_table(connection, table_name):
                metadata.create_all(engine)
        engine.dispose()
    except Exception as e:
        print(f"Error Drop and create new table named {table_name}. Error: {e}")
        raise
    
@task(log_prints=True)
def get_data_from_database(table_name):
    try:
        query = f"SELECT * FROM {table_name}" 
        df = pd.read_sql(query, con=engine)        
        df.columns = df.columns.str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
        df.columns = [f'Column_{i}' if col.startswith('unnamed:') else col for i, col in enumerate(df.columns)]
        return df
    except Exception as e:
        print(f"Error Get data of table named {table_name}. Error: {e}")
        raise
    
def read_data_into_dataframe(data_file_path):
    try:
        df = pd.read_csv(data_file_path).astype(str)
        df.columns = [f'Column_{i}' if col.startswith('Unnamed:') else col for i, col in enumerate(df.columns)]
        df.columns = df.columns.str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
        return df
    except Exception as e:
        print(f"Error read_data_into_dataframe. Error: {e}")
        raise

@task()
def extract(chunk_size: int, source_table: str):
    logger = get_run_logger()
    try:

        query = f"SELECT * FROM {source_table}"
        df = pd.read_sql(query, con=engine)
        return dd.from_pandas(df, chunksize=chunk_size)
    except Exception as e:
        logger.error(f"Error extract data from table {source_table}: {e}")
        raise

@task()
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

def thread_info():
    return threading.current_thread().name    

@task(log_prints=True)
def load(data, chunk_size, target_table):
    print(f"Loading partition in thread: {thread_info()}")
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

@task(log_prints=True)
def save_to_db(df, table_name, chunk_size, max_workers):
    try:
        ddf = dd.from_pandas(df, chunksize=chunk_size)
        ddf_partitions = ddf.to_delayed()
        # for part in ddf_partitions:
        #     load(part, chunk_size, table_name)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(load, partition, chunk_size, table_name) for partition in ddf_partitions]
            for future in futures:
                future.result()  

        # load.map(ddf_partitions, chunk_size, table_name)
    except Exception as e:
        print(f"Error save data to database. Error: {e}")
        raise

def get_current_timestamp():
    current_time = datetime.now()
    nanoseconds = int(time.time() * 1e9) % 1e9
    formatted_timestamp = current_time.strftime(f"%Y-%m-%d %H:%M:%S.{int(nanoseconds):09d}")
    return formatted_timestamp

def process_data(df):
    # # Add your processing logic here
    # df['processed_column'] = df['column_name'].apply(lambda x: x * 2)  # Example processing
    return df
    
@task(log_prints=True)
def add_column_to_db(df, column_name): 
    df[column_name] = [get_current_timestamp()] * len(df) 
    return df

@flow(log_prints=True)
def etl_07_flow(table_name_db_to_fetch, target_table_etl_07_project_data, chunk_size, max_workers):     
    df = get_data_from_database.with_options(name=f"Get data of table named {table_name_db_to_fetch}")(table_name_db_to_fetch)
    column_name_to_add = "insert_time"
    df = add_column_to_db.with_options(name=f"Add column named insert_time to data")(df, column_name_to_add)
    drop_and_create_table.with_options(name=f"Drop and create new table named {target_table_etl_07_project_data}")(df, target_table_etl_07_project_data)
    save_to_db.with_options(name=f"Save data to database named {target_table_etl_07_project_data}")(df, target_table_etl_07_project_data, chunk_size, max_workers)
    
@flow(log_prints=True)
def etl_07_flow_check_integrity():
    target_table_etl_07_project_data = "target_table_etl_07_project_data"
    
    df = get_data_from_database.with_options(name=f"Get data of table named {target_table_etl_07_project_data}")(target_table_etl_07_project_data)
    print(f"Data in table named {target_table_etl_07_project_data} is:\n", df)
    print("Data of Column named insert_time is:\n", df["insert_time"])
    

if __name__ == "__main__":  
    batch_size = 1000  
    chunk_size = 5000
    max_workers = 2 
    table_name_db_to_fetch = "srcadm_project_data"
    target_table_etl_07_project_data = "target_table_etl_07_project_data"
    
    # # Run local
    # etl_07_flow(table_name_db_to_fetch, target_table_etl_07_project_data, chunk_size, max_workers)
    # etl_07_flow_check_integrity()
    
    # Run in deployment
    etl_07 = etl_07_flow.to_deployment(name="etl_07", 
                                       parameters={Params.TABLE_NAME_TO_FETCH: table_name_db_to_fetch,
                                                Params.TARGET_TABLE_ETL_07_PROJECT_DATA: target_table_etl_07_project_data,
                                                Params.CHUNK_SIZE: chunk_size,
                                                Params.MAX_WORKKERS: max_workers})
    etl_07_check_integrity = etl_07_flow_check_integrity.to_deployment(name="etl_07_check_integrity")
    serve(etl_07, etl_07_check_integrity)

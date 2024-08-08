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
        df = pd.read_sql(query, con=connection_string)
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

@task(log_prints=True)
def get_missing_rows(table_name_project_data, table_name_regionname_africa):
    try:
        # task_1 = get_data_from_database.with_options(name=f"Get data of table named {table_name_project_data}").submit(table_name_project_data)
        # task_2 = get_data_from_database.with_options(name=f"Get data of table named {table_name_regionname_africa}").submit(table_name_regionname_africa)
        
        # df_project_data = task_1.result()
        # df_regionname_africa = task_2.result()
        
        # df_missing_rows = df_project_data[~df_project_data.isin(df_regionname_africa.to_dict(orient='list')).all(axis=1)]
        
        query_row_missing = f"""
        SELECT A.*
        FROM {table_name_project_data} A
        LEFT JOIN {table_name_regionname_africa} B 
        ON DBMS_LOB.SUBSTR(A.id, 4000) = DBMS_LOB.SUBSTR(B.id, 4000)
        WHERE B.id IS NULL
        """
        df_missing_rows = pd.read_sql(query_row_missing, con=connection_string)
        print("The difference in the number of records between the project_data table and the regionname_africa table:", len(df_missing_rows))
        
        return df_missing_rows
    except Exception as e:
        print(f"Error Get missing rows between two data tables named {table_name_project_data} and {table_name_regionname_africa}. Error: {e}")
        raise
    
@flow(log_prints=True)
def etl_05_flow(data_file_path, chunk_size, max_workers):     
    df = read_data_into_dataframe(data_file_path)
    
    df_regionname_africa = df[df["regionname"] == "Africa"]
    print("The number of records of srcadm.project_data:", len(df))
    print("The number of records where the column regionname is equal to 'Africa':", len(df_regionname_africa))
    
    table_name_regionname_africa = "tgtadm_project_data_restore"
    table_name_project_data = "srcadm_project_data"
    
    # tgtadm_project_data_restore_missing_rows = "tgtadm_project_data_restore_missing_rows"
    
    task_1 = drop_and_create_table.with_options(name=f"Drop and create new table named {table_name_regionname_africa}").submit(df_regionname_africa, table_name_regionname_africa) 
    task_2 = drop_and_create_table.with_options(name=f"Drop and create new table named {table_name_project_data}").submit(df, table_name_project_data)
    
    # task_3 = drop_and_create_table.with_options(name=f"Drop and create new table named {tgtadm_project_data_restore_missing_rows}").submit(df, tgtadm_project_data_restore_missing_rows)    
    
    task_1.result()
    task_2.result()
    
    # task_3.result()
    
    task_4 = save_to_db.with_options(name=f"Save data to database named {table_name_regionname_africa}").submit(df_regionname_africa, table_name_regionname_africa, chunk_size, max_workers)   
    task_5 = save_to_db.with_options(name=f"Save data to database named {table_name_project_data}").submit(df, table_name_project_data, chunk_size, max_workers) 
    
    task_4.result()
    task_5.result()
    
    df_missing_rows = get_missing_rows.with_options(name=f"Get missing rows between two data tables named {table_name_project_data} and {table_name_regionname_africa}")(table_name_project_data, table_name_regionname_africa)    
    save_to_db.with_options(name=f"Save data to database named {table_name_regionname_africa}")(df_missing_rows, table_name_regionname_africa, chunk_size, max_workers)
    
@flow(log_prints=True)
def etl_05_flow_check_integrity():
    table_name_regionname_africa = "tgtadm_project_data_restore"
    table_name_project_data = "srcadm_project_data"
    tgtadm_project_data_restore_missing_rows = "tgtadm_project_data_restore_missing_rows"
    
    task_1 = get_data_from_database.with_options(name=f"Get data of table named {table_name_regionname_africa}").submit(table_name_regionname_africa)
    task_2 = get_data_from_database.with_options(name=f"Get data of table named {table_name_project_data}").submit(table_name_project_data)
    task_3 = get_data_from_database.with_options(name=f"Get data of table named {tgtadm_project_data_restore_missing_rows}").submit(tgtadm_project_data_restore_missing_rows)
    
    df_regionname_africa = task_1.result()
    df_project_data = task_2.result()
    df_project_data_restore_missing_rows = task_3.result()
    
    print(f"Data in table named {table_name_regionname_africa} is:\n", df_regionname_africa)
    print(f"Data in table named {table_name_project_data} is:\n", df_project_data)
    print(f"Data in table named {tgtadm_project_data_restore_missing_rows} is:\n", df_project_data_restore_missing_rows)


if __name__ == "__main__":    
    data_file_path = './projects_data.csv'
    chunk_size = 5000
    max_workers = 28 
    
    # # Run local
    # etl_05_flow(data_file_path, chunk_size, max_workers)
    # etl_05_flow_check_integrity()
    
    # Run in deployment
    etl_05 = etl_05_flow.to_deployment(name="etl_05", parameters={
                                                Params.DATA_FILE_PATH: data_file_path,
                                                Params.CHUNK_SIZE: chunk_size,
                                                Params.MAX_WORKKERS: max_workers})
    etl_05_check_integrity = etl_05_flow_check_integrity.to_deployment(name="etl_05_check_integrity")
    serve(etl_05, etl_05_check_integrity)

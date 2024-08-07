import paramiko
import pandas as pd
import cx_Oracle
import io
from prefect import flow, task, serve, get_run_logger
from config import settings
from constant import Params
from sqlalchemy import create_engine, text, Table, MetaData, Column, String, CLOB
import dask.dataframe as dd
from dask import delayed
import dask
from concurrent.futures import ThreadPoolExecutor
import threading


# cx_Oracle.init_oracle_client("C:\oracle\instantclient-basic-windows.x64-23.4.0.24.05\instantclient_23_4")

host = settings.ORACLE_HOST
port = settings.ORACLE_PORT
sid = settings.ORACLE_SID
user = settings.ORACLE_USER
password = settings.ORACLE_PASSWORD

connection_string = f'oracle+cx_oracle://{user}:{password}@{host}:{port}/{sid}'
engine = create_engine(connection_string)

@task(log_prints=True)
def get_csv_via_ssh(server_ip, username, password, command, port):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(server_ip, username=username, password=password, port=port)
        stdin, stdout, stderr = client.exec_command(command)
        # csv_data = stdout.read().decode('utf-8')
        csv_data = stdout.read().decode('Latin-1')
        client.close()
        df = pd.read_csv(io.StringIO(csv_data))
        df.columns = df.columns.str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
        return df
    except Exception as e:
        print(f"Error getting the .csv file via SSH from server {server_ip}. Error: {e}")
        raise    

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
        return df
    except Exception as e:
        print(f"Error Get data of table named {table_name}. Error: {e}")
        raise

def mapping_and_refine_data(df, value_mapping):
    try:
        # Add CODE column based on DEALSIZE using the value mapping
        df = df.merge(value_mapping, left_on='dealsize', right_on='dealsize', how='left')
        refined_df = df[['ordernumber', 'quantityordered', 'orderdate', 'year_id', 'status', 'msrp', 'productcode', 'customername', 'code']]
        return refined_df
    except Exception as e:
        print(f"Error mapping_and_refine_data. Error: {e}")
        raise

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
def process_data_and_save_to_database(server_ip,
                             server_username,
                             server_password,
                             server_port,
                             command_show_data,
                             table_name_db_to_fetch,
                             table_name_db_to_save, 
                             chunk_size, 
                             max_workers
                             ):
    task_1 = get_data_from_database.with_options(name=f"Get data of table named {table_name_db_to_fetch}").submit(table_name_db_to_fetch)
    # Retrieve CSV data from server
    task_2 = get_csv_via_ssh.with_options(name=f"Connect to SSH on server {server_ip} and get data").submit(server_ip, server_username, server_password, command_show_data, server_port)
    
    value_mapping_code_and_dealsize = task_1.result()
    data_from_server = task_2.result()
    print("Value mapping between DEALSIZE and CODE:\n", value_mapping_code_and_dealsize)
    print(f"Data from server {server_ip}:\n", data_from_server.head())

    # Enrich and refine data
    refine_data_from_server = mapping_and_refine_data(data_from_server, value_mapping_code_and_dealsize)
    print(f"Post-processing data from server {server_ip}:\n", refine_data_from_server.head())
    
    drop_and_create_table.with_options(name=f"Drop and create new table named {table_name_db_to_save}")(refine_data_from_server, table_name_db_to_save)
    # Save data concurrently
    save_to_db.with_options(name=f"Save data to database named {table_name_db_to_save}")(refine_data_from_server, table_name_db_to_save, chunk_size, max_workers)

@flow(log_prints=True)
def etl_01_flow(table_name_db_to_fetch, file_path_data_on_server_1, file_path_data_on_server_2, target_table_etl_01_1, target_table_etl_01_2, chunk_size, max_workers):
    # server_1 info
    server_ip_1 = settings.SERVER_IP_1
    server_username_1 = settings.SERVER_USERNAME_1
    server_password_1 = settings.SERVER_PASSWORD_1
    server_port_1 = settings.SERVER_PORT_1 
    command_show_data_1 = "cat " + file_path_data_on_server_1

    # server_2 info
    server_ip_2 = settings.SERVER_IP_2
    server_username_2 = settings.SERVER_USERNAME_2
    server_password_2 = settings.SERVER_PASSWORD_2
    server_port_2 = settings.SERVER_PORT_2
    command_show_data_2 = "cat " + file_path_data_on_server_2
    
    process_data_and_save_to_database.with_options(name=f"Processing data and save to database named {target_table_etl_01_1}").submit(server_ip_1, server_username_1, server_password_1, server_port_1, command_show_data_1, table_name_db_to_fetch, target_table_etl_01_1, chunk_size, max_workers)   
    process_data_and_save_to_database.with_options(name=f"Processing data and save to database named {target_table_etl_01_2}").submit(server_ip_2, server_username_2, server_password_2, server_port_2, command_show_data_2, table_name_db_to_fetch, target_table_etl_01_2, chunk_size, max_workers) 

@flow(log_prints=True)
def etl_01_flow_check_integrity():
    target_table_etl_01_1 = Params.TARGET_TABLE_ETL_01_1
    target_table_etl_01_2 = Params.TARGET_TABLE_ETL_01_2
    
    task_1 = get_data_from_database.with_options(name=f"Get data of table named {target_table_etl_01_1}").submit(target_table_etl_01_1)
    task_2 = get_data_from_database.with_options(name=f"Get data of table named {target_table_etl_01_2}").submit(target_table_etl_01_2)
    
    target_table_etl_01_1_df = task_1.result()
    target_table_etl_01_2_df = task_2.result()
    
    print(f"Data in table named {target_table_etl_01_1} is:\n", target_table_etl_01_1_df)
    print(f"Data in table named {target_table_etl_01_2} is:\n", target_table_etl_01_2_df)


if __name__ == "__main__":
    chunk_size = 5000
    max_workers = 28 
    
    table_name_db_to_fetch = "ETL_SALES_DATA_SAMPLE_MAPPING"
    file_path_data_on_server_1 = "./csv_data/sales_data_sample_server1.csv"
    file_path_data_on_server_2 = "./csv_data/sales_data_sample_server2.csv"
    target_table_etl_01_1 = Params.TARGET_TABLE_ETL_01_1
    target_table_etl_01_2 = Params.TARGET_TABLE_ETL_01_2
    
    # # Run local
    # etl_01_flow(table_name_db_to_fetch, file_path_data_on_server_1, file_path_data_on_server_2, target_table_etl_01_1, target_table_etl_01_2, chunk_size, max_workers)
    
    # Run in deployment
    etl_01 = etl_01_flow.to_deployment(name="etl_01", 
                                       parameters={Params.TABLE_NAME_TO_FETCH: table_name_db_to_fetch,
                                                   Params.FILE_PATH_DATA_ON_SERVER_1: file_path_data_on_server_1,
                                                   Params.FILE_PATH_DATA_ON_SERVER_2: file_path_data_on_server_2,
                                                   Params.TARGET_TABLE_ETL_01_1: target_table_etl_01_1,
                                                   Params.TARGET_TABLE_ETL_01_2: target_table_etl_01_2,
                                                   Params.CHUNK_SIZE: chunk_size,
                                                    Params.MAX_WORKKERS: max_workers})
    etl_01_check_integrity = etl_01_flow_check_integrity.to_deployment(name="etl_01_check_integrity")
    serve(etl_01, etl_01_check_integrity)
    
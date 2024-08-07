import pandas as pd
import cx_Oracle
from prefect import flow, task, serve, get_run_logger
from config import settings
from constant import Params
import requests 
import dask.dataframe as dd
from sqlalchemy import create_engine, text, Table, MetaData, Column, String, CLOB
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

@task(retries=2, retry_delay_seconds=10)
def obtain_data_from_api(api_url: str):
    data = None
    status_code = None
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        status_code = response.status_code
    except requests.exceptions.RequestException as e:
        print(f"Error obtain data from API {api_url}. Error: {e}")
        # After the retries, return the current values of data and status_code
        status_code = response.status_code if 'response' in locals() else None
    return data, status_code

@task(log_prints=True)
def change_status_voc_data(merged_df, status_arbitrary_api):
    try:
        for index, row in merged_df.iterrows():
            status = row["status_voc_jira"]            
            if (status == "Close" or status == "Resolve"):
                if status_arbitrary_api == 200:
                    # update voc data
                    merged_df.loc[index, "stat_cd"] = "0004"
            if status == "In Progress" and row["stat_cd"] == "0003":
                # update voc data
                merged_df.loc[index, "stat_cd"] = "0008"
        filter_voc_data = merged_df[["status_voc_jira", "voc_id", "stat_cd", "jira_id", "erp_id"]]
        return filter_voc_data
    except Exception as e:
        print(f"Error change status of voc_data. Error: {e}")
        raise

@task(log_prints=True)
def change_status_erp_data(merged_df):
    try:
        for index, row in merged_df.iterrows():
            status = row["status_voc_jira"]            
            if (status == "Close" or status == "Resolve"):
                # update erp data
                merged_df.loc[index, "status_voc_erp"] = 900 
        filter_voc_erp = merged_df[["status_voc_jira", "erp_id", "status_voc_erp"]]
        return filter_voc_erp
    except Exception as e:
        print(f"Error change status of erp_data. Error: {e}")
        raise

def get_filter_voc_data_df(voc_data_df):
    try:
        # Retrieve items with status 0003 and 0008
        filter_voc_data_df = voc_data_df[voc_data_df['stat_cd'].isin(['0003', '0008'])]
        return filter_voc_data_df
    except Exception as e:
        print(f"Error get_filter_voc_data_df. Error: {e}")
        raise
    
def get_filter_voc_erp_df(filter_voc_data_df, voc_erp):
    try:
        erp_col = list(filter_voc_data_df["erp_id"])
        filter_voc_erp_df = pd.DataFrame([item for item in voc_erp if item["id"] in erp_col])
        # Rename 'id' column in filter_voc_erp_df to match 'ERP_ID' in filter_voc_data_df
        filter_voc_erp_df.rename(columns={'id': 'erp_id', "status": "status_voc_erp"}, inplace=True)
        return filter_voc_erp_df
    except Exception as e:
        print(f"Error get_filter_voc_erp_df. Error: {e}")
        raise  

def get_filter_voc_jira_df(filter_voc_data_df, voc_jira):
    try:
        jira_col = list(filter_voc_data_df["jira_id"])   
        filter_voc_jira = [item for item in voc_jira if item["key"] in jira_col]
        filter_voc_jira_df = pd.DataFrame([{"jira_id": item["key"],
                                "status_voc_jira": item["fields"]["status"]["name"]} 
                                for item in filter_voc_jira]) 
        return filter_voc_jira_df
    except Exception as e:
        print(f"Error get_filter_voc_jira_df. Error: {e}")
        raise

def combine_one_data_df(filter_voc_data_df, filter_voc_erp_df, filter_voc_jira_df):
    try:
        # Merge filter_voc_data_df with filter_voc_erp_df on 'erp_id'
        merged_df = pd.merge(filter_voc_data_df, filter_voc_erp_df, on='erp_id', how='left')
        # Merge filter_voc_data_df with filter_voc_jira_df on 'jira_id'
        merged_df = pd.merge(merged_df, filter_voc_jira_df, on='jira_id', how='left')
        return merged_df
    except Exception as e:
        print(f"Error combine_one_data_df. Error: {e}")
        raise

@flow(log_prints=True)
def etl_03_flow(table_name_db_to_fetch, target_table_etl_03_voc_data, target_table_etl_03_erp_data, api_url_arbitrary_to_check_status, chunk_size, max_workers):
    # Retrieve data from Oracle database
    voc_data_table_obj = get_data_from_database.with_options(name=f"Get data of table named {table_name_db_to_fetch}").submit(table_name_db_to_fetch)
    
    api_url_voc_erp = "http://20.247.162.44/api/v1/voc_erp"
    api_url_voc_jira = "http://20.247.162.44/api/v1/voc_jira"
    
    task_1 = obtain_data_from_api.with_options(name=f"Get data of voc_erp.json from API: {api_url_voc_erp}").submit(api_url_voc_erp)
    task_2 = obtain_data_from_api.with_options(name=f"Get data of voc_jira.json from API: {api_url_voc_jira}").submit(api_url_voc_jira)
    task_3 = obtain_data_from_api.with_options(name=f"Get status of arbitrary API: {api_url_arbitrary_to_check_status}").submit(api_url_arbitrary_to_check_status)
    
    voc_data_df = voc_data_table_obj.result()
    voc_erp_df, _ = task_1.result()
    voc_jira_df, _ = task_2.result()
    _, status_arbitrary_api = task_3.result()
    
    print("Data in voc_data:\n", voc_data_df)
    print("Data in voc_erp:\n", voc_erp_df)
    print("Data in voc_jira:\n", voc_jira_df)
    
    filter_voc_data_df = get_filter_voc_data_df(voc_data_df)
    filter_voc_erp_df = get_filter_voc_erp_df(filter_voc_data_df, voc_erp_df)
    filter_voc_jira_df = get_filter_voc_jira_df(filter_voc_data_df, voc_jira_df)
        
    merged_df = combine_one_data_df(filter_voc_data_df, filter_voc_erp_df, filter_voc_jira_df)
    
    task_4 = change_status_erp_data.with_options(name=f"Change status relating to erp_data").submit(merged_df)
    task_5 = change_status_voc_data.with_options(name=f"Change status relating to voc_data").submit(merged_df, status_arbitrary_api)
    
    updated_voc_erp = task_4.result()
    updated_voc_data = task_5.result()
    
    print("Data in voc_erp after changing status:\n", updated_voc_erp)
    print("Data in voc_data after changing status:\n", updated_voc_data)

    task_6 = drop_and_create_table.with_options(name=f"Drop and create new table named {target_table_etl_03_voc_data}").submit(updated_voc_data, target_table_etl_03_voc_data)
    task_7 = drop_and_create_table.with_options(name=f"Drop and create new table named {target_table_etl_03_erp_data}").submit(updated_voc_erp, target_table_etl_03_erp_data)
    
    task_6.result()
    task_7.result()
    
    # Save data concurrently
    save_to_db.with_options(name=f"Save data to database named {target_table_etl_03_voc_data}").submit(updated_voc_data, target_table_etl_03_voc_data, chunk_size, max_workers)
    save_to_db.with_options(name=f"Save data to database named {target_table_etl_03_erp_data}").submit(updated_voc_erp, target_table_etl_03_erp_data, chunk_size, max_workers)
    
@flow(log_prints=True)
def etl_03_flow_check_integrity():
    target_table_etl_03_voc_data = "target_table_etl_03_voc_data"
    target_table_etl_03_erp_data = "target_table_etl_03_erp_data"
    
    task_1 = get_data_from_database.with_options(name=f"Get data of table named {target_table_etl_03_voc_data}").submit(target_table_etl_03_voc_data)
    task_2 = get_data_from_database.with_options(name=f"Get data of table named {target_table_etl_03_erp_data}").submit(target_table_etl_03_erp_data)
    
    voc_data_df = task_1.result()
    erp_data_df = task_2.result()
    
    print(f"Data in table named {target_table_etl_03_voc_data} is:\n", voc_data_df)
    print(f"Data in table named {target_table_etl_03_erp_data} is:\n", erp_data_df)


if __name__ == "__main__":
    chunk_size = 5000
    max_workers = 28 
    
    table_name_db_to_fetch = "ETL_03_VOC_DATA"
    target_table_etl_03_voc_data = "target_table_etl_03_voc_data"
    target_table_etl_03_erp_data = "target_table_etl_03_erp_data"
    api_url_arbitrary_to_check_status = "http://20.247.162.44/status/200"
    
    # # Run local
    # etl_03_flow(table_name_db_to_fetch, target_table_etl_03_voc_data, target_table_etl_03_erp_data, api_url_arbitrary_to_check_status, chunk_size, max_workers)
    # etl_03_flow_check_integrity()

    # Run in deployment
    etl_03 = etl_03_flow.to_deployment(name="etl_03",
                                    parameters={
                                                Params.TABLE_NAME_TO_FETCH: table_name_db_to_fetch, 
                                                Params.TARGET_TABLE_ETL_03_VOC_DATA: target_table_etl_03_voc_data,
                                                Params.TARGET_TABLE_ETL_03_ERP_DATA: target_table_etl_03_erp_data,
                                                Params.API_URL_ARBITRARY_TO_CHECK_STATUS: api_url_arbitrary_to_check_status,
                                                Params.CHUNK_SIZE: chunk_size,
                                                Params.MAX_WORKKERS: max_workers})
    etl_03_check_integrity = etl_03_flow_check_integrity.to_deployment(name="etl_03_check_integrity")
    serve(etl_03, etl_03_check_integrity)

    # # call an arbitrary api and return the status
    # api_url_200 = "http://20.247.162.44/status/200"
    # api_url_500 = "http://20.247.162.44/status/500"
    
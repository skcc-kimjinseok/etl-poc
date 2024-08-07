from prefect import flow, task, get_run_logger
import requests
import cx_Oracle
import pandas as pd
import os
from sqlalchemy.dialects.oracle import FLOAT as OracleFLOAT
from sqlalchemy import create_engine, Float, String

database_config = {
    "DB_HOST": os.environ.get("DB_HOST"),
    "DB_PORT": os.environ.get("DB_PORT"),
    "DB_USER": os.environ.get("DB_USER"),
    "DB_PASSWORD": os.environ.get("DB_PASSWORD"),
    "DB_NAME": os.environ.get("DB_NAME"),
}

ORACLE_DB_CONNECTION_STRING = f"oracle+cx_oracle://{database_config['DB_USER']}:{database_config['DB_PASSWORD']}@{database_config['DB_HOST']}:{database_config['DB_PORT']}/{database_config['DB_NAME']}"
VALUE_MAPPING_TABLE = "etl_expense_mapping"
TARGET_TABLE = "TGT_EXPENSE_CODE_AMOUNT"
API_URL = "http://20.24.161.42/api/v1/cost-sample"

engine = create_engine(ORACLE_DB_CONNECTION_STRING)
dsn = cx_Oracle.makedsn(
    f"{database_config['DB_HOST']}",
    int(database_config["DB_PORT"]),
    service_name=f"{database_config['DB_NAME']}",
)


@task(retries=2, retry_delay_seconds=10)
def obtain_data_from_api(api_url: str):
    logger = get_run_logger()
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Extract data from api {api_url}:\n {data}")
        return data
    except Exception as e:
        logger.error(f"Extract data from api {api_url} error: {e}")
        raise


@task
def fetch_data_mapping(table_name: str):
    logger = get_run_logger()
    try:
        query = f"SELECT WHAT, CODE FROM {table_name}"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            mapping_dict = df.set_index("what")["code"].to_dict()

            logger.info(
                f"fetch value mapping from table {table_name}: \n{mapping_dict}"
            )

        return mapping_dict
    except Exception as e:
        logger.error(f"fetch value maping from table {table_name} error: {e}")
        raise


@task
def substitute_json_data(json_data, value_mapping):
    logger = get_run_logger()
    for who in json_data:
        for week in who["WEEK"]:
            for expense in week["EXPENSE"]:
                expense["CODE"] = value_mapping[expense["WHAT"]]
    logger.info(f"data transform:\n {json_data}")

    return json_data


@task
def extract_code_amount(expense_data):
    logger = get_run_logger()
    data = []
    for who in expense_data:
        for week in who["WEEK"]:
            for expense in week["EXPENSE"]:
                data.append((expense["CODE"], expense["AMOUNT"]))
    df = pd.DataFrame(data, columns=["CODE", "AMOUNT"])

    logger.info(f"Extract code amount successfully:")
    logger.info(df)

    return df


@task
def save_to_db(data_frame, table_name):
    logger = get_run_logger()
    try:
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

        with engine.connect() as connection:
            summed_data = data_frame.groupby("CODE")["AMOUNT"].sum().reset_index()
            column_types = {
                "CODE": String(50),
                "AMOUNT": Float(precision=53).with_variant(
                    OracleFLOAT(binary_precision=126), "oracle"
                ),
            }
            summed_data.to_sql(
                table_name,
                connection,
                if_exists="replace",
                index=False,
                dtype=column_types,
            )

            logger.info(f"Save data to table {table_name} successfully")
    except Exception as e:
        logger.error(f"Save data to table {table_name} error: {e}")
        raise


@flow(name="etl-02-pipeline")
def etl_02_flow(
    api_url: str = API_URL,
    value_mapping_table: str = VALUE_MAPPING_TABLE,
    tgt_table: str = TARGET_TABLE,
):
    api_data = obtain_data_from_api(api_url)
    mapping_data = fetch_data_mapping(value_mapping_table)
    transform_data = substitute_json_data(api_data, mapping_data)
    extract_data = extract_code_amount(transform_data)
    save_to_db(extract_data, tgt_table)


if __name__ == "__main__":
    etl_02_flow.serve(name="etl_02_pipeline")

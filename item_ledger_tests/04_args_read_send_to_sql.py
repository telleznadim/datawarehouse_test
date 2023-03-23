import pyodbc
from dotenv import dotenv_values
import pandas as pd
from tqdm import tqdm
import timeit
from datetime import datetime, timedelta
import sys
import logging

config = dotenv_values(
    "../.env")

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def define_logger(file_name):
    # create a file handler and set the logging level
    handler = logging.FileHandler(
        f'{file_name}.log')
    handler.setLevel(logging.DEBUG)

    # create a formatter and add it to the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s : %(message)s")
    handler.setFormatter(formatter)

    # add the handler to the logger
    logger.addHandler(handler)
    return (logger)


def delete_records(table_name, erp, company_short):
    logger.debug(
        f'Deleting records WHERE (dw_erp_system = {erp} AND dw_evi_bu = {company_short}')
    server = config["sql_server"]
    database = config["sql_database"]
    uid = config["sql_uid"]
    pwd = config["sql_pwd"]

    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};'
                          'Server=tcp:' + server + ';'
                          'Database=' + database + ';'
                          'Uid={' + uid + '};'
                          'Pwd={' + pwd + '};'
                          'Encrypt=yes;'
                          'TrustServerCertificate=no;'
                          )

    cursor = conn.cursor()

    cursor.execute(f'''
                    DELETE FROM {table_name}
                    WHERE (dw_erp_system = '{erp}' AND dw_evi_bu = '{company_short}')
                ''')

    conn.commit()
    conn.close()


def read_records(table_name, erp, company_short):
    logger.debug(
        f'Deleting records WHERE (dw_erp_system = {erp} AND dw_evi_bu = {company_short}')
    server = config["sql_server"]
    database = config["sql_database"]
    uid = config["sql_uid"]
    pwd = config["sql_pwd"]

    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};'
                          'Server=tcp:' + server + ';'
                          'Database=' + database + ';'
                          'Uid={' + uid + '};'
                          'Pwd={' + pwd + '};'
                          'Encrypt=yes;'
                          'TrustServerCertificate=no;'
                          )

    sql_query = f'''
                    SELECT entry_no FROM {table_name}
                    WHERE (dw_erp_system = '{erp}' AND dw_evi_bu = '{company_short}')
                '''
    # cursor.execute(f'''
    #                 SELECT entry_no FROM {table_name}
    #                 WHERE (dw_erp_system = '{erp}' AND dw_evi_bu = '{company_short}')
    #             ''')
    df = pd.read_sql_query(sql_query, conn)

    conn.commit()
    conn.close()

    return (df)


def columns_to_string(df, table_name):
    columns_list = df.columns
    rows_string = ""
    columns_string = "("
    question_string = " values("
    for i in range(0, len(columns_list)):
        logger.debug(columns_list[i])
        if (i == len(columns_list) - 1):
            columns_string = f'{columns_string}{columns_list[i]})'
            question_string = f'{question_string}?)'
            rows_string = f'{rows_string}row.{columns_list[i]}'
        else:
            columns_string = f'{columns_string}{columns_list[i]}, '
            question_string = f'{question_string}?,'
            rows_string = f'{rows_string}row.{columns_list[i]}, '

    logger.debug(rows_string)
    return (f'INSERT INTO {table_name} {columns_string}{question_string}')


def select_db_table(conn, df, table_name):
    cursor = conn.cursor()
    insert_string = columns_to_string(df, table_name)
    logger.debug(insert_string)
    logger.debug("Inserting data to SQL Server")
    if (table_name == "dw_test_locations"):
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            cursor.execute(insert_string, row.location_code, row.location_name, row.dw_evi_bu,
                           row.dw_erp_system, row.dw_erp_source_table, row.dw_timestamp, row.dw_location_id)
    elif ((table_name == "dw_test_item_ledger_entries") | (table_name == "dw_test_item_ledger_test")):
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            cursor.execute(insert_string, row.entry_no, row.entry_type, row.document_type, row.document_no, row.item_no, row.item_description, row.global_dimension_1_code, row.global_dimension_2_code,
                           row.location_code, row.quantity, row.remaining_quantity, row.invoiced_quantity, row.dw_evi_bu, row.dw_erp_system, row.dw_erp_source_table, row.dw_timestamp, row.dw_item_ledger_entry_id)
    elif (table_name == "dw_test_purchase_lines"):
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            cursor.execute(insert_string, row.document_type, row.document_no, row.line_no, row.buy_from_vendor_no, row.type, row.item_no, row.item_description,
                           row.location_code, row.quantity, row.outstanding_quantity, row.dw_evi_bu, row.dw_erp_system, row.dw_erp_source_table, row.dw_timestamp, row.dw_purchase_line_id)
    elif (table_name == "dw_test_sales_lines"):
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            cursor.execute(insert_string, row.document_type, row.document_no, row.line_no, row.sell_to_customer_no, row.type, row.item_no, row.item_description,
                           row.location_code, row.quantity, row.outstanding_quantity, row.dw_evi_bu, row.dw_erp_system, row.dw_erp_source_table, row.dw_timestamp, row.dw_sales_line_id)
    elif (table_name == "dw_test_items"):
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            cursor.execute(insert_string, row.item_no, row.item_description, row.item_status, row.inventory_posting_group, row.gen_prod_posting_group, row.vendor_no,
                           row.vendor_item_no, row.item_category_code, row.brand, row.dw_evi_bu, row.dw_erp_system, row.dw_erp_source_table, row.dw_timestamp, row.dw_item_id)

    conn.commit()
    cursor.close()


def insert_to_dw(df, table_name):
    server = config["sql_server"]
    database = config["sql_database"]
    uid = config["sql_uid"]
    pwd = config["sql_pwd"]

    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};'
                          'Server=tcp:' + server + ';'
                          'Database=' + database + ';'
                          'Uid={' + uid + '};'
                          'Pwd={' + pwd + '};'
                          'Encrypt=yes;'
                          'TrustServerCertificate=no;'
                          )

    select_db_table(conn, df, table_name)

    logger.debug(f"Inserting data process COMPLETED for {table_name}")
    conn.close()


def read_and_insert_to_dw(company, endpoint, date_time_string, sql_table):
    logger.debug(
        f'Reading ... {company}_{endpoint}_{date_time_string}.csv.gz file')

    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/files/from_datalake/{company}_{endpoint}_{date_time_string}.csv.gz',
        compression="gzip")
    logger.debug(df['dw_timestamp'])
    # df = df.apply(transform_df, axis=1)

    # df['dw_timestamp'] = pd.to_datetime(df['dw_timestamp'])
    df.fillna("", inplace=True)
    logger.debug(df)
    # logger.debug(df["dw_timestamp"])
    insert_to_dw(df, sql_table)
    #
    # logger.debug(insert_string)


def read_csv_and_insert_to_dw_2(company, endpoint, date_time_string, sql_table, df_sqlserver, column_name):
    logger.debug(
        f'Reading ... {company}_{endpoint}_{date_time_string}.csv.gz file')

    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/test/item_ledger_tests/files/from_datalake/{company}_{endpoint}_{date_time_string}.csv.gz',
        compression="gzip")
    df['entry_no'] = df['entry_no'].astype(str)

    print(df["entry_no"])
    print(df_sqlserver["entry_no"])

    df = df[~df[column_name].isin(df_sqlserver[column_name])]
    logger.debug("Dataframe after filtering:")

    print(df)
    df.fillna("", inplace=True)
    logger.debug(df)
    # logger.debug(df["dw_timestamp"])
    insert_to_dw(df, sql_table)


def main():
    date_time = datetime.now()

    if (len(sys.argv) == 3):

        company_short = sys.argv[1]
        endpoint = sys.argv[2]
        logger = define_logger(
            f"04_read_send_to_sql_{company_short}_{endpoint}")
        logger.debug(f'-------- Executing 04 read_send_to_sql ---------')
        logger.debug(f'Datetime = {date_time.strftime("%m-%d-%y %H:%M:%S") }')
        logger.debug(f'company_short = {company_short}, endpoint = {endpoint}')
        companies = {"TRS": "TRSPROD01", "FLR": "FLOPROD", "CTL": "CTLPROD"}

        data_to_read = {
            "Items": "dw_test_items",
            "Locations": "dw_test_locations",
            "ItemLedgerEntries": "dw_test_item_ledger_entries",
            "PurchaseLines": "dw_test_purchase_lines",
            "SalesLines": "dw_test_sales_lines",
            "ItemLedgerTest": "dw_test_item_ledger_test"
        }
        logger.debug(data_to_read[endpoint])
        logger.debug(company_short)

        df_sqlserver = read_records(
            data_to_read[endpoint], "BC", company_short)
        read_csv_and_insert_to_dw_2(company_short, endpoint, date_time.strftime(
            "%m%d%y"), data_to_read[endpoint], df_sqlserver, "entry_no")

        # delete_records(data_to_read[endpoint], "BC", company_short)
        # read_and_insert_to_dw(
        #     company_short, endpoint, date_time.strftime("%m%d%y"), data_to_read[endpoint])


if __name__ == '__main__':
    start = timeit.default_timer()
    main()
    end = timeit.default_timer()
    logger.debug(f'Duration: {end-start} secs')
    logger.debug(f'Duration: {(end-start)/60} mins')

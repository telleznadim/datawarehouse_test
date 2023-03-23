import pyodbc
from dotenv import dotenv_values
import pandas as pd
from tqdm import tqdm
import timeit
from datetime import datetime

config = dotenv_values(".env")


def transform_df(df):
    # print(str(df["Invoice Date"]))
    # date_time = datetime.date.fromtimestamp(df["dw_timestamp"])
    df["dw_timestamp"] = datetime.strftime(
        datetime.fromtimestamp(df["dw_timestamp"] / 1e3), "%Y/%m/%d %H:%M:%S")
    # print(str(df["dw_timestamp"]).strftime("%Y"))
    # df["item_no"] = f"PAC_{df.item_no}"
    # df["inventory_division_no"] = f"PAC_{df.inventory_division_no}"
    # df["inventory_class_no"] = f"PAC_{df.inventory_class_no}"
    return (df)


def columns_to_string(df, table_name):
    columns_list = df.columns
    rows_string = ""
    columns_string = "("
    question_string = " values("
    for i in range(0, len(columns_list)):
        print(columns_list[i])
        if (i == len(columns_list) - 1):
            columns_string = f'{columns_string}{columns_list[i]})'
            question_string = f'{question_string}?)'
            rows_string = f'{rows_string}row.{columns_list[i]}'
        else:
            columns_string = f'{columns_string}{columns_list[i]}, '
            question_string = f'{question_string}?,'
            rows_string = f'{rows_string}row.{columns_list[i]}, '

    print(rows_string)
    return (f'INSERT INTO {table_name} {columns_string}{question_string}')


def select_db_table(conn, df, table_name):
    if (table_name == "dw_test_locations"):
        insert_string = columns_to_string(df, table_name)
        print(insert_string)
        cursor = conn.cursor()
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            print(
                f'test: {datetime.strftime(datetime.fromtimestamp(row.dw_timestamp / 1e3), "%Y/%m/%d %H:%M:%S")}')
            cursor.execute(insert_string, row.location_code, row.location_name, row.dw_evi_bu,
                           row.dw_erp_system, row.dw_erp_source_table, row.dw_location_id, datetime.now())
        conn.commit()
        cursor.close()
    elif (table_name == "dw_test_item_ledger_entries"):
        insert_string = columns_to_string(df, table_name)
        # print(insert_string)
        cursor = conn.cursor()
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            cursor.execute(insert_string, row.entry_no, row.entry_type, row.document_type, row.document_no, row.item_no, row.item_description, row.global_dimension_1_code, row.global_dimension_2_code,
                           row.location_code, row.quantity, row.remaining_quantity, row.invoiced_quantity, row.dw_evi_bu, row.dw_erp_system, row.dw_erp_source_table, row.dw_item_ledger_entry_id, row.dw_timestamp)
        conn.commit()
        cursor.close()
    elif (table_name == "dw_test_purchase_lines"):
        insert_string = columns_to_string(df, table_name)
        # print(insert_string)
        cursor = conn.cursor()
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            cursor.execute(insert_string, row.document_type, row.document_no, row.line_no, row.buy_from_vendor_no, row.type, row.item_no, row.item_description,
                           row.location_code, row.quantity, row.outstanding_quantity, row.dw_evi_bu, row.dw_erp_system, row.dw_erp_source_table, row.dw_purchase_line_id, row.dw_timestamp)
        conn.commit()
        cursor.close()
    elif (table_name == "dw_test_sales_lines"):
        insert_string = columns_to_string(df, table_name)
        # print(insert_string)
        cursor = conn.cursor()
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            cursor.execute(insert_string, row.document_type, row.document_no, row.line_no, row.sell_to_customer_no, row.type, row.item_no, row.item_description,
                           row.location_code, row.quantity, row.outstanding_quantity, row.dw_evi_bu, row.dw_erp_system, row.dw_erp_source_table, row.dw_sales_line_id, row.dw_timestamp)
        conn.commit()
        cursor.close()


def insert_to_dw():
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
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        cursor.execute(insert_string, row.document_type, row.document_no, row.line_no, row.buy_from_vendor_no, row.type, row.item_no, row.item_description,
                       row.location_code, row.quantity, row.outstanding_quantity, row.dw_evi_bu, row.dw_erp_system, row.dw_erp_source_table, row.dw_purchase_line_id, row.dw_timestamp)
    conn.commit()
    cursor.close()

    conn.close()


def read_and_insert_to_dw(company, endpoint, date_time_string, sql_table):
    print(f'Reading ... {company}_{endpoint}_{date_time_string}.csv.gz file')

    df = pd.read_csv(
        f'../../DataLake/test/files/from_datalake/{company}_{endpoint}_{date_time_string}.csv.gz',
        compression="gzip")
    # df = df.apply(transform_df, axis=1)
    # df['dw_timestamp'] = pd.to_datetime(df['dw_timestamp'])
    df.fillna("", inplace=True)
    print(df)
    # print(df["dw_timestamp"])
    insert_to_dw(df, sql_table)
    #
    # print(insert_string)


def main():
    insert_to_dw()
    # date_time_string = "011123_131355"
    # # date_time = datetime.now()
    # data_to_read = [
    #     # {"endpoint": "Items", "company": "TRS"},
    #     {"endpoint": "Locations", "company": "TRS",
    #         "sql_table": "dw_test_locations"},
    #     # {"endpoint": "ItemLedgerEntries", "company": "TRS",
    #     #     "sql_table": "dw_test_item_ledger_entries"},
    #     # {"endpoint": "PurchaseLines", "company": "TRS",
    #     #     "sql_table": "dw_test_purchase_lines"},
    #     # {"endpoint": "SalesLines", "company": "TRS",
    #     #     "sql_table": "dw_test_sales_lines"},
    # ]
    # for data in data_to_read:
    #     read_and_insert_to_dw(
    #         data["company"], data["endpoint"], date_time_string, data["sql_table"])


if __name__ == '__main__':
    start = timeit.default_timer()
    main()
    end = timeit.default_timer()
    print("Duration: ", end-start, "secs")
    print("Duration: ", (end-start)/60, "mins")

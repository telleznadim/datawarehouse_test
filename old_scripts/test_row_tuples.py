from collections import namedtuple

# Define the names of the columns in the table
row_string_list = ["document_no", "line_no", "sell_to_customer_no", "type"]

# Create a named tuple class with the column names as field names
CreditMemoLine = namedtuple('CreditMemoLine', row_string_list)
print(CreditMemoLine)

# Create an instance of the named tuple with values for each field
row = CreditMemoLine('CM0001', 1, 'CUST001', 'Return')
print(row)

# Generate the SQL query string with placeholders for each column
insert_string = "INSERT INTO dw_test_credit_memo_lines ({}) VALUES ({})".format(
    ','.join(row_string_list), ','.join(['?' for _ in row_string_list]))

print(insert_string)
print("LOL{}".format("TEST"))

# Pass the generated SQL query string and the row values to the execute method
# cursor.execute(insert_string, row)

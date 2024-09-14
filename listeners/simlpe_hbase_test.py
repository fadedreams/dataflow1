import happybase

# Connect to HBase
connection = happybase.Connection('hbase')  # Replace 'localhost' with your HBase host if different

# Create a table if it doesn't exist
table_name = 'my_table'
column_family = 'cf1'

if table_name not in connection.tables():
    connection.create_table(
        table_name,
        {column_family: dict()}
    )

# Get a reference to the table
table = connection.table(table_name)

# Write data to the table
row_key = 'row1'
data = {
    f'{column_family}:column1': 'value1',
    f'{column_family}:column2': 'value2'
}

table.put(row_key, data)

# Verify data was written
row = table.row(row_key)
print(row)

# Close the connection
connection.close()


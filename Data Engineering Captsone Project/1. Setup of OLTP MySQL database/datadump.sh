#!/bin/bash

# MySQL connection information
mysql_host="172.21.214.75"
mysql_port="3306"
mysql_user="root"
mysql_password="9Y76YPtt1XmQfMvoKiPaTbXs"
mysql_database="sales"
mysql_table="sales_data"

# Create the SQL file
sql_file="sales_data.sql"
touch "$sql_file"

# Connect to MySQL and dump the table data
mysql -h "$mysql_host" -P "$mysql_port" -u "$mysql_user" -p"$mysql_password" "$mysql_database" <<EOF
USE $mysql_database;
SELECT * FROM $mysql_table INTO OUTFILE '$sql_file';
EOF

# Check if the dump was successful
if [ $? -eq 0 ]; then
  echo "Data exported successfully to $sql_file"
else
  echo "Error exporting data"
fi
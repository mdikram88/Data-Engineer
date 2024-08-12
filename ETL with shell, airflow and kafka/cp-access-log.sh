# cp-access-log.sh
# This script downloads the file 'web-server-access-log.txt.gz'
# from "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/".

# The script then extracts the .txt file using gunzip.

# The .txt file contains the timestamp, latitude, longitude 
# and visitor id apart from other data.

# Transforms the text delimeter from "#" to "," and saves to a csv file.
# Loads the data from the CSV file into the table 'access_log' in PostgreSQL database.

wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"

# Unzip the file to extract the .txt file.
gunzip -f web-server-access-log.txt.gz


echo "Extracting data"


cut -d"#" -f1-4 web-server-access-log.txt > extracted-data2.txt

echo "Tranforming data"

tr "#" "," < extracted-data2.txt > transformed_data2.csv


# Load phase
echo "Loading data"

# Send the instructions to connect to 'template1' and
# copy the file to the table 'access_log' through command pipeline.

echo "\c template1;\COPY access_log  FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost

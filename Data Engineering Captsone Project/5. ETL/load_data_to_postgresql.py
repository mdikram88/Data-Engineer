from sqlalchemy import create_engine
import psycopg2
import pandas as pd

user="postgres"
password = "Eu2Ok00tl1CmLKuMCD5Gx2h4"
host = "172.21.14.240"
port = "5432"

database = "postgres"

url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(url)
conn = engine.connect()


sales_data = pd.read_csv("sales-csv3mo8i5SHvta76u7DzUfhiw.csv")
print(sales_data.head(2))
print(sales_data.to_sql("sales_data", conn, if_exists="replace", index=False))


conn.close()

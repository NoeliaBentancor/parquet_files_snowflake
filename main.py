import os
from snowflake import connector
from dotenv import load_dotenv
import pandas as pd
load_dotenv()
database=os.getenv("SNOWSQL_DATABASE")
snowflake_conn =connector.connect(
      authenticator="snowflake",
      user=os.getenv("SNOWSQL_USER"),
      password=os.getenv("SNOWSQL_PASSWORD"),
      account=os.getenv("SNOWSQL_ACCOUNT"),
      database=os.getenv("SNOWSQL_DATABASE"),
      schema=os.getenv("SNOWSQL_SCHEMA"),
      warehouse=os.getenv("SNOWSQL_WAREHOUSE"))
snowflake_conn_cursor=snowflake_conn.cursor()
snowflake_conn_cursor.execute("USE SCHEMA {}".format(os.getenv("SNOWSQL_SCHEMA")))
snowflake_conn_cursor.execute("USE WAREHOUSE {}".format(os.getenv("SNOWSQL_WAREHOUSE")))

path =str(os.getenv("FILES_PATH"))
dir_list = os.listdir(path)
print(dir_list)
stagename=str(os.getenv("SNOWSQL_STAGE"))
snowflake_conn_cursor.execute("CREATE STAGE IF NOT EXISTS {}" .format(stagename))
for file in dir_list:
    snowflake_conn_cursor.execute("PUT file://"+path+"/"+file+" @"+stagename+"/"+file)
    snowflake_conn_cursor.execute("CREATE OR REPLACE TABLE "+file.split(".parquet")[0]  +  " (PARQUET_RAW VARIANT)")
    print("COPY INTO " +database+ "." +file.split(".parquet")[0]+ " FROM @"+stagename+"/"+file+" FILE_FORMAT = (TYPE = PARQUET)")
    snowflake_conn_cursor.execute("COPY INTO " +file.split(".parquet")[0]+ " FROM @"+stagename+"/"+file+" FILE_FORMAT = (TYPE = PARQUET)")
rawtable = "RAW_PARQUET_DATA"
ubicacion= snowflake_conn_cursor.execute("SELECT * FROM ubicacion")
for ubic in ubicacion:
    print(ubic)
print(ubicacion)


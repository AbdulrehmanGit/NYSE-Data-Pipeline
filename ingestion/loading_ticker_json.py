
from utillity import spark_session,get_pg_connection
from pyspark.sql.functions import col,when
import os
import psycopg2
from  dotenv import load_dotenv
from pyspark.sql.types import *

json_path="D:/Workspace/NYSE/data/raw/tickers"

load_dotenv()

jdbc_url = os.getenv("jdbc_url")
ticker_table = os.getenv("ticker_table")
properties = {
    "user":os.getenv('user'),
    "password":os.getenv('password'),
    "driver":os.getenv('driver')
}



conn = get_pg_connection()

curr =conn.cursor()
curr.execute("select filename from nyse_stg.metadata_loaded_files")
loaded_files = set(row[0] for row in curr.fetchall())
curr.close()

all_files =[f for f in os.listdir(json_path) if f.endswith(".json")]
new_files =[f for f in all_files if f not in loaded_files]



# Exit early if no new files
if not new_files:
    print("No new files to load.")
else:
    print(f"Found {len(new_files)} new files: {new_files}")

    spark = spark_session("NYSE")

    # Schema for individual ticker record
    ticker_schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("name", StringType(), True),
        StructField("market", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("primary_exchange", StringType(), True),
        StructField("type", StringType(), True),
        StructField("active", BooleanType(), True),
        StructField("currency_name", StringType(), True),
        StructField("cik", StringType(), True),
        StructField("composite_figi", StringType(), True),
        StructField("share_class_figi", StringType(), True),
        StructField("last_updated_utc", StringType(), True)  # Optionally TimestampType
    ])

    # Full schema for the JSON file
    full_schema = StructType([
        StructField("results", ArrayType(ticker_schema), True),
        StructField("status", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("count", IntegerType(), True),
        StructField("next_url", StringType(), True)
    ])

    path= [os.path.join(json_path,file) for file in  new_files]
    raw_df = spark.read.schema(full_schema).option("multiline","true").json(path)
    
    tickers_df =raw_df.selectExpr("explode(results) as results").select("results.*")
    tickers_df = tickers_df.dropDuplicates(["ticker"])

    tickers_df = tickers_df.withColumn("last_updated_utc", col("last_updated_utc").cast("timestamp"))

    existing_landed_tickers = spark.read.jdbc(
    url=jdbc_url,
    table = ticker_table,
    
    properties=properties
    )

    new_landing_df= tickers_df.alias('new').join(
    existing_landed_tickers.select("ticker").alias('existing'),
    on='ticker',
    how='left_anti'
    )

    new_landing_df.write.jdbc(
    url = os.getenv('jdbc_url'),
    table= ticker_table,
    mode= 'append',
    properties = properties
    )

    con =get_pg_connection()
    curr = conn.cursor()
    for filename in new_files:
        curr.execute("insert into nyse_stg.metadata_loaded_files (filename) values (%s)",(filename,))
    conn.commit()
    conn.close()

    print("New files loaded and metadata updated.")

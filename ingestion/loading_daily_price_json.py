
from utillity import spark_session,get_pg_connection
from pyspark.sql.functions import col,when,explode,from_unixtime
import shutil
from pathlib import Path
import os
import psycopg2
from  dotenv import load_dotenv
from pyspark.sql.types import *

json_path=Path("D:/Workspace/NYSE/data/raw/daily_agg")
archive_path = json_path /"archive"
archive_path.mkdir(exist_ok=True)
load_dotenv()
all_files = [f for f in os.listdir(json_path) if f.endswith(".json") and os.path.isfile(json_path/f)]


if not all_files:
    print("No new Files to load")
    
else:
        
    print(f"found {len(all_files)} new files: {all_files}")
    spark = spark_session("NYSE")

    daily_agg_schema = StructType([
        StructField("T", StringType(), True),   # Ticker
        StructField("v", DoubleType(), True),   # Volume
        StructField("vw", DoubleType(), True),  # Volume Weighted Avg Price
        StructField("o", DoubleType(), True),   # Open
        StructField("c", DoubleType(), True),   # Close
        StructField("h", DoubleType(), True),   # High
        StructField("l", DoubleType(), True),   # Low
        StructField("t", LongType(), True),     # Timestamp
        StructField("n", IntegerType(), True)   # Number of transactions
    ])
    full_schema = StructType([
        StructField("results", ArrayType(daily_agg_schema), True),
        StructField("status", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("count", IntegerType(), True)
    ])


    file_paths =[str(json_path/f) for  f in all_files]
    print(file_paths)
    spark.conf.set("spark.sql.caseSensitive", "true")

    raw_df = spark.read.schema(full_schema).option("multiline","true").json(file_paths)

    agg_df = (
        raw_df
        .selectExpr("explode(results) as results")
        .select(
            col("results.T").alias("ticker"),
            col("results.v").alias("volume"),
            col("results.vw").alias("vwap"),
            col("results.o").alias("open"),
            col("results.c").alias("close"),
            col("results.h").alias("high"),
            col("results.l").alias("low"),
            col("results.t").alias("timestamp"),
            col("results.n").alias("transactions")
        )
        .dropDuplicates(["ticker", "timestamp"])
    )

    agg_df.show()

    agg_df = agg_df.withColumn("timestamp",from_unixtime(col("timestamp")/1000).cast("timestamp"))

    agg_df
    agg_df.show()

    agg_df.count()


    daily_aggregate_table = os.getenv("daily_agg_table")
    properties = {
        "user":os.getenv('user'),
        "password":os.getenv('password'),
        "driver":os.getenv('driver')
    }

    agg_df.write.jdbc(
        url = os.getenv('jdbc_url'),
        table= daily_aggregate_table,
        mode= 'append',
        properties = properties
    )
    for f in all_files:
        src =json_path/f
        dst =archive_path/f
        shutil.move(src,dst)
        print(f"Moved {f} to archive")
    print("all new daily aggregate files loaded and archived successfully")
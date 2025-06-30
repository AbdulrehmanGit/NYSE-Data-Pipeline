
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()
def spark_session(app_name):
    import os
    os.environ["HADOOP_HOME"] = "D:/Workspace/ETL/hadoop-3.2.2"
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .config("spark.hadoop.hadoop.native.io", "false") \
        .getOrCreate()

# jdbc_url = "jdbc:postgresql://192.168.0.180:5432/postgres"
# properties = {
#     "user": "postgres",
#     "password": "postgres",
#     "driver": "org.postgresql.Driver"
# }

def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv("dbname"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port")
    )

def log_ingestion(table_name, date_ingested):
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO nyse_stg.dailyprice_loaded_file (table_name, ingestion_date)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
    """, (table_name, date_ingested))
    conn.commit()
    cur.close()
    conn.close()

def is_date_already_loaded(log_table, date_str):
    con =get_pg_connection()
    with con.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {log_table} WHERE ingestion_date = %s LIMIT 1", 
            (date_str,)
        )
        return cur.fetchone() is not None

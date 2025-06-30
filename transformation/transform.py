from utillity import spark_session,get_pg_connection
import psycopg2
from  dotenv import load_dotenv
from pyspark.sql.types import *
import os
from pyspark.sql.functions import col,when,lower,to_date,year,month,day,weekday,date_format,dayofmonth,weekofyear
from datetime import datetime
import pandas as pd


today = datetime.today()
date_str = today.strftime("%Y-%m-%d")
load_dotenv()

daily_ticker = os.getenv("ticker_table")
daily_price = os.getenv("daily_agg_table")


nyse_date_table =os.getenv("nyse_date_table")
nyse_ticker_table =os.getenv("nyse_ticker_table") 
nyse_fact_table =os.getenv("nyse_fact_table")


jdbc_url = os.getenv('jdbc_url')
properties = {
        "user":os.getenv('user'),
        "password":os.getenv('password'),
        "driver":os.getenv('driver')
    }

spark = spark_session('NYSE')

# load from stg 
ticker_df =  spark.read.jdbc(
    url=jdbc_url,
    table = daily_ticker,
    properties=properties
)


# load from stg
daily_price_df =  spark.read.jdbc(
    url=jdbc_url,
    table = daily_price,
    properties=properties
)

print("Loading Data from STG")
print(daily_price_df.show())

dim_ticker = ticker_df \
    .dropDuplicates(['ticker']) \
    .withColumn('market',lower(col('market'))) \
    .withColumn('type',lower(col('type'))) \
    # .withColumn('ticker_id',monotonically_increasing_id())
dim_ticker = dim_ticker.select(['ticker','market','type'])

date_only =  spark.read.jdbc(
    url=jdbc_url,
    table = os.getenv("nyse_date_table"),
    properties=properties
)
date_only = date_only.select('date')

print("previous dates that have been loaded")
print(date_only.show())


dim_date = daily_price_df \
    .withColumn("date", to_date("timestamp")) \
    .select("date", "timestamp") \
    .distinct() \
    .withColumn("year", year("timestamp")) \
    .withColumn("month", month("timestamp")) \
    .withColumn("day", dayofmonth("timestamp")) \
    .withColumn("weekday", date_format("timestamp", "E")) \
    .withColumn("weekyear", weekofyear("timestamp")) \
    .select("date", "year", "month", "weekyear", "weekday", "day")


dim_date = dim_date.select(['date','year','month','weekyear','weekday','day'])
dim_date = dim_date.join(date_only,on='date',how='left_anti')


print("New date that is going to be loaded")
print(date_only.show())


# ###########################################################################
# deduped_daily_price_df = daily_price_df.dropDuplicates(['ticker','timestamp'])

# deduped_dim_ticker = dim_ticker.dropDuplicates(['ticker'])


# fact_daily_price_df  = deduped_daily_price_df.withColumn("date",to_date("timestamp")) \
#                         .join(dim_date,on='date',how="inner") \
#                         .join(dim_ticker.select('ticker'),on='ticker',how='inner') \
#                         .select(['ticker','date', "open", "close", "high", "low", "volume", "vwap", "transactions"])
# # fact_daily_price_df = fact_daily_price_df.dropDuplicates(['ticker','date'])

# print(fact_daily_price_df.count())
# fact_daily_price_df.count()
# print(dim_date.count())

# deduped_dim_date = dim_date.dropDuplicates(['date'])

# deduped_dim_date.write.jdbc(
#     url=jdbc_url,
#     table=nyse_date_table,
#     mode='append',
#     properties=properties)
# print("Data Loaded into NYSE DATE DIMENSION")

# existing_dim_ticker = spark.read.jdbc(
#     url= jdbc_url,
#     table = nyse_ticker_table,
#     properties=properties
# )

# new_landing_df= deduped_dim_ticker.alias('new').join(
#     existing_dim_ticker.select("ticker").alias('existing'),
#     on='ticker',
#     how='left_anti'
#     )
# new_landing_df.show()

# new_landing_df.write.jdbc(url=jdbc_url,
#                               table=nyse_ticker_table,
#                               mode='append',
#                               properties=properties)
# print("Data Loaded into NYSE TICKER DIMENSION")

print(daily_price_df.count())

# 1 Dropping Duplicates
deduped_daily_price_df = daily_price_df.dropDuplicates(['ticker','timestamp'])
deduped_dim_ticker = dim_ticker.dropDuplicates(['ticker'])

deduped_dim_date = dim_date.dropDuplicates(['date']).cache()
deduped_dim_date.show()
#  2 Loading data into  warehouse
deduped_dim_date.write.jdbc(
    url=jdbc_url,
    table=nyse_date_table,
    mode='append',
    properties=properties)
print("Data Loaded into NYSE DATE DIMENSION")

# reading ticker data from warehouse 
existing_dim_ticker = spark.read.jdbc(
    url= jdbc_url,
    table = nyse_ticker_table,
    properties=properties
)

# identifying new  tickers

    
new_landing_df= deduped_dim_ticker.alias('new').join(
    existing_dim_ticker.select("ticker").alias('existing'),
    on='ticker',
    how='left_anti'
    )
print(new_landing_df.show())

if new_landing_df.count() >0:
    new_landing_df.write.jdbc(url=jdbc_url,
                                table=nyse_ticker_table,
                                mode='append',
                                properties=properties)
    print("New Tickers added into NYSE TICKER DIMENSION")
else:
    print('no new ticker')


# 6. ⚡ Now Reload the updated ticker dimension table (very important)
updated_dim_ticker = spark.read.jdbc(
    url=jdbc_url,
    table=nyse_ticker_table,
    properties=properties
)
                        
fact_daily_price_df  = deduped_daily_price_df.withColumn("date",to_date("timestamp")) \
                        .join(deduped_dim_date,on='date',how="inner") \
                        .join(updated_dim_ticker.select('ticker'),on='ticker',how='inner') \
                        .select(['ticker','date', "open", "close", "high", "low", "volume", "vwap", "transactions"])
print(fact_daily_price_df.count())
# fact_daily_price_df = fact_daily_price_df.dropDuplicates(['date'])

fact_daily_price_df.count()
# fact_daily_price_df = fact_daily_price_df[fact_daily_price_df['ticker']=='AACT']
fact_daily_price_df.show()
fact_daily_price_df.dropDuplicates(['ticker'])

fact_daily_price_df.count()

# ###########################################################################


# Commenting for Testing Purpose
fact_daily_price_df.write.jdbc(url=jdbc_url,
                               table=nyse_fact_table,
                               mode='append',
                               properties=properties)
print("Data Loaded into NYSE DAILY SALE FACT")

# Commenting for Testing Purpose

deduped_dim_date =deduped_dim_date.withColumn('date',col('date').cast("string"))
fact_daily_price_df = fact_daily_price_df.withColumn('date',col('date').cast("string"))

print(fact_daily_price_df.show())

deduped_dim_date = deduped_dim_date.toPandas()
updated_dim_ticker =deduped_dim_ticker.toPandas()

fact_daily_price_df = fact_daily_price_df.toPandas()

parquet_path = os.getenv("ticker_parquet_path")
new_ticker_data = updated_dim_ticker
# .to_parquet(f"D:/Workspace/NYSE/data/prepared/dim_ticker/dim_ticker_{date_str}.parquet", index=False)
if os.path.exists(parquet_path):
    ticker_existing_data =pd.read_parquet(parquet_path)
    ticker_combined_data = pd.concat([ticker_existing_data,new_ticker_data],ignore_index=True)
else:
    ticker_combined_data = new_ticker_data
final_data = ticker_combined_data.drop_duplicates()
final_data.to_parquet(parquet_path,index=False)

deduped_dim_date.to_parquet(f"D:/Workspace/NYSE/data/prepared/dim_date/dim_date_{date_str}.parquet", index=False)

fact_daily_price_df.to_parquet(f"D:/Workspace/NYSE/data/prepared/fact_daily_price/fact_daily_price_{date_str}.parquet", index=False)
print(fact_daily_price_df.head)
#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def init_spark():
    sp=SparkSession.builder \
        .master('spark://spark-master:7077') \
        .appName('Submit-App-2') \
        .config('spark.driver.memory', '512M')  \
        .config('spark.executor.memory', '512M')  \
        .config('spark.executor.instances', '1')  \
        .config('spark.executor.cores', '1')  \
        .config("spark.cores.max", "1") \
        .config("spark.jars", "postgresql-42.7.3.jar")\
        .getOrCreate()
    sc = sp.sparkContext
    return sp,sc
spark,sc = init_spark()

def main():
  global   spark,sc   
  url = "jdbc:postgresql://postgres-server:5432/stocks"
  properties = {
    "user": "Test",
    "password": "Test123",
    "driver": "org.postgresql.Driver"
  }

  # Define the schema
  schema = StructType([
      StructField("id", StringType(), True),
      StructField("content", StringType(), True),
      StructField("sendTime", TimestampType(), True),
      StructField("sendTimePersian", StringType(), True),
      StructField("senderName", StringType(), True),
      StructField("senderUsername", StringType(), True),
      StructField("type", StringType(), True),
      StructField("hashtags", StringType(), True)
  ])

  directory = "/opt/bitnami/spark/apps/csv"

  # Read all CSV files in the directory
  df = spark.read.format("csv").option("sep", "\t").option("header", "true").schema(schema).load(directory + "/*.csv")

  new_df = df.withColumn("sendTime", to_timestamp(col("sendTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

  # Debugging: print the number of rows after filtering
  print("Number of rows:", df.count())
  df.show(5)
  print("-="*30)
  
  if new_df.count() > 0:
        new_df.write.jdbc(url=url, table="tweets", mode='append', properties=properties)
        print("Data written to the database.")
  else:
        print("No valid data to write to the database.")
    
if __name__ == '__main__':
  main()

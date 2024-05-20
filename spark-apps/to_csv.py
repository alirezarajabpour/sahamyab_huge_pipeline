#!/usr/bin/env python
# coding: utf-8

import uuid

TOPIC_Step2_NAME="Sahamyab-Tweets2"
KAFKA_SERVER="kafka1:19092,kafka2:19093,kafka3:19094"

import os
# https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
# https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12
# setup arguments
os.environ['PYSPARK_SUBMIT_ARGS']='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField


spark = SparkSession.builder\
         .master("spark://spark-master:7077")\
         .appName("Step2_04-Hashtag-Count-Console")\
         .config("spark.executor.memory", "500mb")\
         .config("spark.executor.cores","1")\
         .config("spark.cores.max", "1")\
         .config("spark.sql.session.timeZone", "Asia/Tehran")\
         .getOrCreate()    
    

spark.sparkContext.setLogLevel("ERROR")
schema = StructType([StructField("id", StringType(), True),\
                     StructField("content", StringType(), True),\
                     StructField("sendTime", StringType(), True), \
                     StructField("sendTimePersian", StringType(), True),\
                     StructField("senderName", StringType(), True),\
                     StructField("senderUsername", StringType(), True),\
                     StructField("type", StringType(), True),\
                     StructField("hashtags", ArrayType(StringType()), True)
                    ])

group_id = "save-csv-" + str(uuid.uuid4())

df = spark\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers", KAFKA_SERVER)\
   .option("subscribe", TOPIC_Step2_NAME)\
   .option("startingOffsets", "earliest")\
   .option("failOnDataLoss", "false") \
   .option("kafka.group.id", "group_id")\
   .load()

df.printSchema()

tweetsStringDF = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

tweetsDF = tweetsStringDF.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Define a UDF to convert array to string
def array_to_string(my_list):
    return ','.join(my_list)

array_to_string_udf = udf(array_to_string, StringType())

# Use the UDF to change the column type
tweetsDF = tweetsDF.withColumn("hashtags", array_to_string_udf(col("hashtags")))

# Define the path where you want to save the CSV file
csv_path = "/opt/bitnami/spark/apps/csv"

# Save the DataFrame as a CSV file with a tab as the delimiter
query = tweetsDF \
    .coalesce(1)\
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", csv_path) \
    .option("checkpointLocation", "/opt/bitnami/spark/apps"+"/csv") \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .start()

print("waiting for termination!")

query.awaitTermination()

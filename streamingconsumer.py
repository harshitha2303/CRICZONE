from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import random

# Create a SparkSession object
spark = SparkSession.builder \
    .appName("CricketStreaming") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema for the incoming cricket data from Kafka
schema = StructType([
    StructField("PlayerID", IntegerType()),
    StructField("RunsScored", IntegerType()),
    StructField("WicketsTaken", IntegerType()),
    StructField("YearsOfExperience", IntegerType()),
    StructField("DateTime", TimestampType())
])

# Define the Kafka topics related to cricket data
topics = {
    "top_runs": "top_runs",
    "top_wickets": "top_wickets",
    "scores": "scores"
}

# Randomly choose a topic
chosen_topic = random.choice(list(topics.keys()))

# Define the Kafka broker details
kafka_bootstrap_servers = "localhost:9092"

# Read data from the chosen Kafka topic into a streaming DataFrame
df_chosen_topic = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topics[chosen_topic]) \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Perform processing, aggregations, and other operations as needed
# For example, you can join this DataFrame with other data sources, apply transformations, etc.

# Write the output to the console
console_query_chosen_topic = df_chosen_topic \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for the query to terminate
console_query_chosen_topic.awaitTermination()

# Stop the query
console_query_chosen_topic.stop()


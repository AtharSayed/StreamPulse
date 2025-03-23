from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json, when

# Create Spark Session with Kafka
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
    .getOrCreate()

# Kafka Configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "ecom-transactions"

# Define Kafka Source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Define Schema for Incoming JSON Data
schema = StructType() \
    .add("InvoiceNo", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", IntegerType()) \
    .add("InvoiceDate", StringType()) \
    .add("UnitPrice", DoubleType()) \
    .add("CustomerID", StringType()) \
    .add("Country", StringType())

# Convert Kafka Message Value from JSON and Apply Schema
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Handle Missing Values with Spark
df_cleaned = df_parsed \
    .withColumn("CustomerID", when(col("CustomerID").isNull(), "Unknown").otherwise(col("CustomerID"))) \
    .withColumn("Description", when(col("Description").isNull(), "No Description").otherwise(col("Description"))) \
    .withColumn("Country", when(col("Country").isNull(), "Unknown").otherwise(col("Country"))) \
    .withColumn("Quantity", when(col("Quantity").isNull(), 0).otherwise(col("Quantity"))) \
    .withColumn("UnitPrice", when(col("UnitPrice").isNull(), 0.0).otherwise(col("UnitPrice")))

# Write Output to Console (or you can write to a database)
query = df_cleaned.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

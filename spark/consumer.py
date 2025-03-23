from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

# Create Spark Session with Kafka
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
    .getOrCreate()

# Define Kafka Source
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "ecom-transactions"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Define Schema (Adjust as per your actual Kafka message format)
schema = StructType() \
    .add("InvoiceNo", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", StringType()) \
    .add("InvoiceDate", StringType()) \
    .add("UnitPrice", StringType()) \
    .add("CustomerID", StringType()) \
    .add("Country", StringType())

# Convert Kafka Message Value from JSON
from pyspark.sql.functions import col, from_json

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write Output to Console (or you can write to a database)
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

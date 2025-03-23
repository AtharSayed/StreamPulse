from confluent_kafka import Producer
from pyspark.sql import SparkSession
import json
import time

# Kafka Configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic = "ecom-transactions"  # ✅ Ensure topic name is consistent across all scripts

# Initialize Spark Session
spark = SparkSession.builder.appName("EcommerceProducer").getOrCreate()

# Load CSV Data Using Spark
csv_path = "E:/DataProcessingProject/data/data.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

# Convert `InvoiceDate` to Proper Datetime Format
df = df.withColumn("InvoiceDate", df["InvoiceDate"].cast("string"))

# Convert DataFrame to JSON and Send to Kafka
for row in df.collect():
    transaction = {
        "InvoiceNo": row["InvoiceNo"],
        "StockCode": row["StockCode"],
        "Description": row["Description"],
        "Quantity": row["Quantity"],
        "InvoiceDate": row["InvoiceDate"],
        "UnitPrice": row["UnitPrice"],
        "CustomerID": row["CustomerID"] if row["CustomerID"] else "Unknown",
        "Country": row["Country"] if row["Country"] else "Unknown"
    }
    producer.produce(topic, key=str(transaction["InvoiceNo"]), value=json.dumps(transaction))
    print(f"Produced: {transaction}")
    time.sleep(0.5)  # Adjust speed as needed

producer.flush()  # ✅ Flush only once at the end
print("✅ Data production completed!")

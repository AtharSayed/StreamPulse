from confluent_kafka import Producer
import pandas as pd
import json
import time

# Kafka Configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic = "ecom-transactions"

# Load CSV Data with Encoding Fix and Header Handling
df = pd.read_csv(r"E:\DataProcessingProject\data\data.csv", encoding="ISO-8859-1", header=0)
df.columns = df.columns.str.strip()  # Removes extra spaces in column names

# Debugging: Print column names and sample data
print("CSV Columns:", df.columns)
print(df.head())

# Convert InvoiceDate to proper datetime format
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format="%m/%d/%Y %H:%M", errors="coerce").astype(str)

# Send each row as a Kafka message
for index, row in df.iterrows():
    transaction = {
        "InvoiceNo": str(row["InvoiceNo"]),
        "StockCode": str(row["StockCode"]),
        "Description": str(row["Description"]),
        "Quantity": int(row["Quantity"]),
        "InvoiceDate": row["InvoiceDate"],  # Already converted to string
        "UnitPrice": float(row["UnitPrice"]),
        "CustomerID": str(row["CustomerID"]) if not pd.isna(row["CustomerID"]) else None,
        "Country": str(row["Country"])
    }
    producer.produce(topic, key=str(transaction["InvoiceNo"]), value=json.dumps(transaction))
    producer.flush()
    print(f"Produced: {transaction}")
    time.sleep(0.5)  # Adjust speed as needed

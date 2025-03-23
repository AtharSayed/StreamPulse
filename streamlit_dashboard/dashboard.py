import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import plotly.express as px
import time
import uuid  # Import uuid to generate unique keys

# Initialize Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("ECommerceAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Define schema for incoming Kafka data
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("Country", StringType(), True)
])

# Read data from Kafka stream
data = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecom-transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka value and apply schema
parsed_data = data.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write streaming data to memory
query = parsed_data.writeStream \
    .format("memory") \
    .queryName("ecommerce_data") \
    .outputMode("append") \
    .start()

# Streamlit dashboard setup
st.set_page_config(page_title="E-Commerce Transactions Dashboard", layout="wide")
st.title("E-Commerce Transactions Dashboard")
st.write("This dashboard displays insights and visualizations from real-time e-commerce transactions.")

# Function to fetch and visualize data
def update_dashboard():
    # Fetch data from the in-memory table
    static_df = spark.sql("SELECT * FROM ecommerce_data")

    if not static_df.isEmpty():
        # Aggregate insights
        total_sales = static_df.groupBy("Country").agg(sum(col("Quantity") * col("UnitPrice")).alias("TotalSales"))
        most_sold_items = static_df.groupBy("Description").agg(sum("Quantity").alias("TotalQuantity")).orderBy(desc("TotalQuantity"))
        top_customers = static_df.groupBy("CustomerID").agg(sum(col("Quantity") * col("UnitPrice")).alias("TotalSpent")).orderBy(desc("TotalSpent"))

        # Convert to Pandas for visualization
        total_sales_pd = total_sales.toPandas()
        most_sold_items_pd = most_sold_items.limit(10).toPandas()
        top_customers_pd = top_customers.limit(10).toPandas()

        # Generate unique keys for each chart
        total_sales_key = f"total_sales_chart_{uuid.uuid4()}"
        most_sold_items_key = f"most_sold_items_chart_{uuid.uuid4()}"
        top_customers_key = f"top_customers_chart_{uuid.uuid4()}"

        # Visualizations
        st.subheader("Total Sales by Country")
        fig1 = px.bar(total_sales_pd, x="Country", y="TotalSales", title="Total Sales by Country", labels={"TotalSales": "Sales (USD)"})
        st.plotly_chart(fig1, use_container_width=True, key=total_sales_key)  # Unique key

        st.subheader("Most Sold Items")
        fig2 = px.bar(most_sold_items_pd, x="Description", y="TotalQuantity", title="Top 10 Most Sold Items")
        st.plotly_chart(fig2, use_container_width=True, key=most_sold_items_key)  # Unique key

        st.subheader("Top 10 Customers")
        fig3 = px.bar(top_customers_pd, x="CustomerID", y="TotalSpent", title="Top 10 Customers by Spending")
        st.plotly_chart(fig3, use_container_width=True, key=top_customers_key)  # Unique key

        st.success("Dashboard Updated with Processed Insights!")
    else:
        st.warning("No data available yet. Waiting for incoming transactions...")

# Periodically update the dashboard
while True:
    update_dashboard()
    time.sleep(5)  # Refresh every 5 seconds
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, from_json, count, avg, date_format
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
        # Existing Insights
        total_sales = static_df.groupBy("Country").agg(sum(col("Quantity") * col("UnitPrice")).alias("TotalSales"))
        most_sold_items = static_df.groupBy("Description").agg(sum("Quantity").alias("TotalQuantity")).orderBy(desc("TotalQuantity"))
        top_customers = static_df.groupBy("CustomerID").agg(sum(col("Quantity") * col("UnitPrice")).alias("TotalSpent")).orderBy(desc("TotalSpent"))

        # New Insights
        # 1. Total Transactions by Country
        total_transactions = static_df.groupBy("Country").agg(count("*").alias("TotalTransactions"))

        # 2. Average Transaction Value by Country
        avg_transaction_value = static_df.groupBy("Country").agg(avg(col("Quantity") * col("UnitPrice")).alias("AvgTransactionValue"))

        # 3. Sales Over Time (Daily)
        sales_over_time = static_df.groupBy(date_format("InvoiceDate", "yyyy-MM-dd").alias("Date")).agg(sum(col("Quantity") * col("UnitPrice")).alias("DailySales"))

        # 4. Top 10 Countries by Sales
        top_countries_sales = total_sales.orderBy(desc("TotalSales")).limit(10)

        # 5. Most Popular Categories (Assuming Description contains category info)
        most_popular_categories = static_df.groupBy("Description").agg(sum("Quantity").alias("TotalQuantity")).orderBy(desc("TotalQuantity")).limit(10)

        # 6. Average Quantity per Transaction by Country
        avg_quantity_per_transaction = static_df.groupBy("Country").agg(avg("Quantity").alias("AvgQuantityPerTransaction"))

        # 7. Customer Retention (Number of Transactions per Customer)
        customer_retention = static_df.groupBy("CustomerID").agg(count("*").alias("NumberOfTransactions")).orderBy(desc("NumberOfTransactions")).limit(10)

        # Convert to Pandas for visualization
        total_sales_pd = total_sales.toPandas()
        most_sold_items_pd = most_sold_items.limit(10).toPandas()
        top_customers_pd = top_customers.limit(10).toPandas()
        total_transactions_pd = total_transactions.toPandas()
        avg_transaction_value_pd = avg_transaction_value.toPandas()
        sales_over_time_pd = sales_over_time.toPandas()
        top_countries_sales_pd = top_countries_sales.toPandas()
        most_popular_categories_pd = most_popular_categories.toPandas()
        avg_quantity_per_transaction_pd = avg_quantity_per_transaction.toPandas()
        customer_retention_pd = customer_retention.toPandas()

        # Generate unique keys for each chart
        total_sales_key = f"total_sales_chart_{uuid.uuid4()}"
        most_sold_items_key = f"most_sold_items_chart_{uuid.uuid4()}"
        top_customers_key = f"top_customers_chart_{uuid.uuid4()}"
        total_transactions_key = f"total_transactions_chart_{uuid.uuid4()}"
        avg_transaction_value_key = f"avg_transaction_value_chart_{uuid.uuid4()}"
        sales_over_time_key = f"sales_over_time_chart_{uuid.uuid4()}"
        top_countries_sales_key = f"top_countries_sales_chart_{uuid.uuid4()}"
        most_popular_categories_key = f"most_popular_categories_chart_{uuid.uuid4()}"
        avg_quantity_per_transaction_key = f"avg_quantity_per_transaction_chart_{uuid.uuid4()}"
        customer_retention_key = f"customer_retention_chart_{uuid.uuid4()}"

        # Visualizations
        st.subheader("Total Sales by Country")
        fig1 = px.bar(total_sales_pd, x="Country", y="TotalSales", title="Total Sales by Country", labels={"TotalSales": "Sales (USD)"})
        st.plotly_chart(fig1, use_container_width=True, key=total_sales_key)

        st.subheader("Most Sold Items")
        fig2 = px.bar(most_sold_items_pd, x="Description", y="TotalQuantity", title="Top 10 Most Sold Items")
        st.plotly_chart(fig2, use_container_width=True, key=most_sold_items_key)

        st.subheader("Top 10 Customers")
        fig3 = px.bar(top_customers_pd, x="CustomerID", y="TotalSpent", title="Top 10 Customers by Spending")
        st.plotly_chart(fig3, use_container_width=True, key=top_customers_key)

        st.subheader("Total Transactions by Country")
        fig4 = px.bar(total_transactions_pd, x="Country", y="TotalTransactions", title="Total Transactions by Country", labels={"TotalTransactions": "Number of Transactions"})
        st.plotly_chart(fig4, use_container_width=True, key=total_transactions_key)

        st.subheader("Average Transaction Value by Country")
        fig5 = px.bar(avg_transaction_value_pd, x="Country", y="AvgTransactionValue", title="Average Transaction Value by Country", labels={"AvgTransactionValue": "Average Value (USD)"})
        st.plotly_chart(fig5, use_container_width=True, key=avg_transaction_value_key)

        st.subheader("Sales Over Time (Daily)")
        fig6 = px.line(sales_over_time_pd, x="Date", y="DailySales", title="Daily Sales Over Time", labels={"DailySales": "Sales (USD)"})
        st.plotly_chart(fig6, use_container_width=True, key=sales_over_time_key)

        st.subheader("Top 10 Countries by Sales")
        fig7 = px.bar(top_countries_sales_pd, x="Country", y="TotalSales", title="Top 10 Countries by Sales", labels={"TotalSales": "Sales (USD)"})
        st.plotly_chart(fig7, use_container_width=True, key=top_countries_sales_key)

        st.subheader("Most Popular Categories")
        fig8 = px.bar(most_popular_categories_pd, x="Description", y="TotalQuantity", title="Top 10 Most Popular Categories", labels={"TotalQuantity": "Total Quantity Sold"})
        st.plotly_chart(fig8, use_container_width=True, key=most_popular_categories_key)

        st.subheader("Average Quantity per Transaction by Country")
        fig9 = px.bar(avg_quantity_per_transaction_pd, x="Country", y="AvgQuantityPerTransaction", title="Average Quantity per Transaction by Country", labels={"AvgQuantityPerTransaction": "Average Quantity"})
        st.plotly_chart(fig9, use_container_width=True, key=avg_quantity_per_transaction_key)

        st.subheader("Customer Retention (Top 10 Customers by Number of Transactions)")
        fig10 = px.bar(customer_retention_pd, x="CustomerID", y="NumberOfTransactions", title="Top 10 Customers by Number of Transactions", labels={"NumberOfTransactions": "Number of Transactions"})
        st.plotly_chart(fig10, use_container_width=True, key=customer_retention_key)

        st.success("Dashboard Updated with Processed Insights!")
    else:
        st.warning("No data available yet. Waiting for incoming transactions...")

# Periodically update the dashboard
while True:
    update_dashboard()
    time.sleep(5)  # Refresh every 5 seconds
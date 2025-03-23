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
st.set_page_config(page_title="E-Commerce Transactions Dashboard", layout="wide", page_icon="📊")

# Custom CSS for styling
st.markdown(
    """
    <style>
    .main {
        background-color: #f5f5f5;
    }
    h1 {
        color: #2e86c1;
        text-align: center;
    }
    h2 {
        color: #148f77;
    }
    .stButton button {
        background-color: #2e86c1;
        color: white;
        border-radius: 5px;
        padding: 10px 20px;
    }
    .stPlotlyChart {
        border-radius: 10px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Title and description
st.title("📊 E-Commerce Transactions Dashboard")
st.markdown("""
    Welcome to the **Real-Time E-Commerce Analytics Dashboard**! This dashboard provides insights into sales, transactions, and customer behavior based on live streaming data.
    """)

# Create placeholders for dynamic updates
total_sales_placeholder = st.empty()
most_sold_items_placeholder = st.empty()
top_customers_placeholder = st.empty()
total_transactions_placeholder = st.empty()
avg_transaction_value_placeholder = st.empty()
sales_over_time_placeholder = st.empty()
most_popular_categories_placeholder = st.empty()
avg_quantity_per_transaction_placeholder = st.empty()
customer_retention_placeholder = st.empty()

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
        total_transactions = static_df.groupBy("Country").agg(count("*").alias("TotalTransactions"))
        avg_transaction_value = static_df.groupBy("Country").agg(avg(col("Quantity") * col("UnitPrice")).alias("AvgTransactionValue"))
        sales_over_time = static_df.groupBy(date_format("InvoiceDate", "yyyy-MM-dd").alias("Date")).agg(sum(col("Quantity") * col("UnitPrice")).alias("DailySales"))
        top_countries_sales = total_sales.orderBy(desc("TotalSales")).limit(10)
        most_popular_categories = static_df.groupBy("Description").agg(sum("Quantity").alias("TotalQuantity")).orderBy(desc("TotalQuantity")).limit(10)
        avg_quantity_per_transaction = static_df.groupBy("Country").agg(avg("Quantity").alias("AvgQuantityPerTransaction"))
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

        # Update placeholders with new data
        with total_sales_placeholder.container():
            st.subheader("🌍 Total Sales by Country")
            fig1 = px.bar(total_sales_pd, x="Country", y="TotalSales", title="Total Sales by Country", labels={"TotalSales": "Sales (USD)"})
            st.plotly_chart(fig1, use_container_width=True, key=f"total_sales_{uuid.uuid4()}")

        with most_sold_items_placeholder.container():
            st.subheader("🛒 Most Sold Items")
            fig2 = px.bar(most_sold_items_pd, x="Description", y="TotalQuantity", title="Top 10 Most Sold Items")
            st.plotly_chart(fig2, use_container_width=True, key=f"most_sold_items_{uuid.uuid4()}")

        with top_customers_placeholder.container():
            st.subheader("💳 Top 10 Customers")
            fig3 = px.bar(top_customers_pd, x="CustomerID", y="TotalSpent", title="Top 10 Customers by Spending")
            st.plotly_chart(fig3, use_container_width=True, key=f"top_customers_{uuid.uuid4()}")

        with total_transactions_placeholder.container():
            st.subheader("🌐 Total Transactions by Country")
            fig4 = px.bar(total_transactions_pd, x="Country", y="TotalTransactions", title="Total Transactions by Country", labels={"TotalTransactions": "Number of Transactions"})
            st.plotly_chart(fig4, use_container_width=True, key=f"total_transactions_{uuid.uuid4()}")

        with avg_transaction_value_placeholder.container():
            st.subheader("💵 Average Transaction Value by Country")
            fig5 = px.bar(avg_transaction_value_pd, x="Country", y="AvgTransactionValue", title="Average Transaction Value by Country", labels={"AvgTransactionValue": "Average Value (USD)"})
            st.plotly_chart(fig5, use_container_width=True, key=f"avg_transaction_value_{uuid.uuid4()}")

        with sales_over_time_placeholder.container():
            st.subheader("📅 Sales Over Time (Daily)")
            fig6 = px.line(sales_over_time_pd, x="Date", y="DailySales", title="Daily Sales Over Time", labels={"DailySales": "Sales (USD)"})
            st.plotly_chart(fig6, use_container_width=True, key=f"sales_over_time_{uuid.uuid4()}")

        with most_popular_categories_placeholder.container():
            st.subheader("📦 Most Popular Categories")
            fig8 = px.bar(most_popular_categories_pd, x="Description", y="TotalQuantity", title="Top 10 Most Popular Categories", labels={"TotalQuantity": "Total Quantity Sold"})
            st.plotly_chart(fig8, use_container_width=True, key=f"most_popular_categories_{uuid.uuid4()}")

        with avg_quantity_per_transaction_placeholder.container():
            st.subheader("📦 Average Quantity per Transaction by Country")
            fig9 = px.bar(avg_quantity_per_transaction_pd, x="Country", y="AvgQuantityPerTransaction", title="Average Quantity per Transaction by Country", labels={"AvgQuantityPerTransaction": "Average Quantity"})
            st.plotly_chart(fig9, use_container_width=True, key=f"avg_quantity_per_transaction_{uuid.uuid4()}")

        with customer_retention_placeholder.container():
            st.subheader("👥 Customer Retention (Top 10 Customers)")
            fig10 = px.bar(customer_retention_pd, x="CustomerID", y="NumberOfTransactions", title="Top 10 Customers by Number of Transactions", labels={"NumberOfTransactions": "Number of Transactions"})
            st.plotly_chart(fig10, use_container_width=True, key=f"customer_retention_{uuid.uuid4()}")

        st.success("✅ Dashboard Updated with Processed Insights!")
    else:
        st.warning("⚠️ No data available yet. Waiting for incoming transactions...")

# Periodically update the dashboard
while True:
    update_dashboard()
    time.sleep(5)  # Refresh every 5 seconds
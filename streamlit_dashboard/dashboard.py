import streamlit as st
import pandas as pd
import plotly.express as px
import time
from kafka import KafkaConsumer
import json

# Streamlit Page Configuration
st.set_page_config(page_title="E-Commerce Transactions Dashboard", layout="wide")

# Custom CSS for Dark Theme
st.markdown("""
    <style>
        body { background-color: #0e1117; color: #ffffff; }
        .main { background-color: #0e1117; }
        h1, h2, h3 { color: #ffffff; }
        .css-18e3th9 { padding-top: 2rem; }
        .stDataFrame { border-radius: 12px; overflow: hidden; }
        .block-container { padding-top: 2rem; }
    </style>
""", unsafe_allow_html=True)

# Sidebar Filters
st.sidebar.header("🔍 Filters")
selected_category = st.sidebar.selectbox("Category", ["All", "Home Decor", "Electronics", "Fashion"])
price_range = st.sidebar.slider("Price Range", 0, 500, (10, 300))

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'ecom-transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Placeholder for real-time data
transactions = []

# Processing Kafka Stream
for message in consumer:
    data = message.value
    transactions.append(data)
    
    # Convert to DataFrame
    df = pd.DataFrame(transactions)

    # Apply Sidebar Filters
    if selected_category != "All":
        df = df[df['Category'] == selected_category]
    df = df[(df['UnitPrice'] >= price_range[0]) & (df['UnitPrice'] <= price_range[1])]

    # Calculate Metrics
    total_sales = df['Quantity'].sum()
    total_revenue = round((df['Quantity'] * df['UnitPrice']).sum(), 2)
    top_product = df['Description'].mode()[0] if not df.empty else "N/A"

    # **DASHBOARD UI**
    st.title("📊 Real-Time E-Commerce Transactions Dashboard")
    
    # Key Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("💰 Total Sales", total_sales)
    col2.metric("🛍️ Total Revenue", f"${total_revenue}")
    col3.metric("🔥 Top Product", top_product)

    # **Live Data Table**
    st.subheader("📡 Live Transactions Stream")
    st.dataframe(df[['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice']])

    # **Interactive Charts**
    st.subheader("📈 Sales Trend")
    sales_chart = px.line(df, x='InvoiceDate', y='Quantity', title="Sales Over Time", markers=True, template="plotly_dark")
    st.plotly_chart(sales_chart, use_container_width=True)

    st.subheader("🛒 Product Distribution")
    product_chart = px.bar(df, x='Description', y='Quantity', title="Top Selling Products", template="plotly_dark")
    st.plotly_chart(product_chart, use_container_width=True)

    # Auto-refresh every 5 seconds
    time.sleep(5)
    st.experimental_rerun()

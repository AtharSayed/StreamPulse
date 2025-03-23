import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

# Set CSV file path
csv_path = r"E:\DataProcessingProject\data\data.csv"

# Load the dataset
def load_data():
    try:
        df = pd.read_csv(csv_path, encoding="ISO-8859-1", header=0)
        return df
    except FileNotFoundError:
        st.error(f"File not found: {csv_path}")
        return None

# Load data
st.title("📊 Real-time E-commerce Transaction Dashboard")
df = load_data()

if df is not None:
    # Convert InvoiceDate to datetime
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
    df.dropna(subset=['InvoiceDate'], inplace=True)
    
    # Calculate total revenue
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
    
    # Sales over time
    sales_over_time = df.groupby(df['InvoiceDate'].dt.date)['TotalPrice'].sum().reset_index()
    st.subheader("📈 Sales Over Time")
    fig1 = px.line(sales_over_time, x='InvoiceDate', y='TotalPrice', title="Total Revenue Trend")
    st.plotly_chart(fig1)
    
    # Top selling products
    top_products = df.groupby('Description')['Quantity'].sum().nlargest(10).reset_index()
    st.subheader("🏆 Top 10 Selling Products")
    fig2 = px.bar(top_products, x='Quantity', y='Description', orientation='h', title="Best-Selling Products")
    st.plotly_chart(fig2)
    
    # Customers by country
    country_sales = df.groupby('Country')['TotalPrice'].sum().nlargest(10).reset_index()
    st.subheader("🌍 Top Countries by Sales")
    fig3 = px.bar(country_sales, x='TotalPrice', y='Country', orientation='h', title="Sales by Country")
    st.plotly_chart(fig3)
    
    # Scatter plot for Quantity vs. Unit Price
    st.subheader("🔍 Quantity vs. Unit Price")
    fig4 = px.scatter(df, x='Quantity', y='UnitPrice', title="Quantity vs. Unit Price", opacity=0.6)
    st.plotly_chart(fig4)
    
    st.success("Dashboard Loaded Successfully! ✅")
else:
    st.error("Unable to load data. Please check the file path.")

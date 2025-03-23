import streamlit as st
from kafka import KafkaConsumer
import pandas as pd
import json

# Streamlit App Title
st.title("Real-time E-commerce Transaction Dashboard")

# Kafka Consumer Setup
KAFKA_TOPIC = "ecommerce_transactions"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Function to Fetch Data from Kafka
@st.cache_data(ttl=60)
def fetch_kafka_data():
    transactions = []
    for message in consumer:
        transactions.append(message.value)
        if len(transactions) > 100:  # Limit messages to avoid memory overload
            break
    return transactions

# Load Data
data = fetch_kafka_data()
df = pd.DataFrame(data)

# Debugging: Check Columns
if df.empty:
    st.warning("No data received from Kafka.")
else:
    st.write("Data received successfully!")

# Check if 'Category' column exists
if 'Category' not in df.columns:
    st.error("Missing 'Category' column. Available columns: " + ", ".join(df.columns))
else:
    # Dropdown for Category Selection
    selected_category = st.selectbox("Select a Category", df['Category'].unique())
    
    # Filter Data by Selected Category
    df = df[df['Category'] == selected_category]

    # Display Data
    st.dataframe(df)

# Add a rerun button to refresh data
if st.button("Refresh Data"):
    st.rerun()

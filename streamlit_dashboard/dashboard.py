import streamlit as st
from kafka import KafkaConsumer
import pandas as pd
import json

# Streamlit App Title
st.title("📊 Real-time E-commerce Transaction Dashboard")

# Kafka Consumer Setup
KAFKA_TOPIC = "ecom-transactions"  # ✅ Ensure topic name matches producer & consumer
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
    consumer.poll(timeout_ms=1000)  # ✅ Avoid infinite loop
    for message in consumer:
        transactions.append(message.value)
        if len(transactions) >= 100:  # ✅ Limit to avoid memory overload
            break
    return transactions

# Load Data
data = fetch_kafka_data()
df = pd.DataFrame(data)

# Debugging: Check Columns
if df.empty:
    st.warning("⚠️ No data received from Kafka.")
    st.stop()  # Stop execution if no data
else:
    st.write("✅ Data received successfully!")

# Check if 'Category' column exists
if 'Category' not in df.columns:
    st.warning("⚠️ Missing 'Category' column. Displaying all data.")
    st.dataframe(df)
else:
    # Dropdown for Category Selection
    selected_category = st.selectbox("Select a Category", df['Category'].unique())

    # Filter Data by Selected Category
    df_filtered = df[df['Category'] == selected_category]
    st.dataframe(df_filtered)

# Add a rerun button to refresh data
if st.button("🔄 Refresh Data"):
    st.rerun()

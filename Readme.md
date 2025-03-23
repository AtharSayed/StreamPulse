# Real-Time E-Commerce Transactions Dashboard

This project implements a real-time dashboard for monitoring e-commerce transactions using Apache Kafka, Apache Spark Structured Streaming, and Streamlit. The system processes and visualizes live transaction data in an interactive web interface.

## Features
- Real-time data streaming using **Apache Kafka**
- Stream processing with **Apache Spark**
- Interactive visualization with **Streamlit** and **Plotly**
- Dark-themed dashboard for better UI/UX

## Technologies Used
- **Apache Kafka** (for real-time messaging)
- **Apache Spark** (for structured streaming)
- **Streamlit** (for web-based visualization)
- **Plotly** (for interactive charts)
- **Python** (for scripting and processing)

## Setup and Installation

### 1. Install Dependencies
Ensure Python is installed, then install the required dependencies:

```sh
pip install -r requirements.txt
```

### 2. Start Kafka Services

#### a. Start Zookeeper
```sh
zookeeper-server-start.bat config/zookeeper.properties
```

#### b. Start Kafka Server
```sh
kafka-server-start.bat config/server.properties
```

### 3. Create Kafka Topic
```sh
kafka-topics.bat --create --topic ecom-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 4. Start the Kafka Producer (Simulating Transactions)
```sh
python producer.py
```

### 5. Start the Spark Streaming Consumer
```sh
python consumer.py
```

### 6. Run the Streamlit Dashboard
```sh
streamlit run dashboard.py
```



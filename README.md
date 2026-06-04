# 🚀 AdTech Real-Time Streaming Pipeline
[![Java](https://img.shields.io/badge/Java-11%2B-blue.svg)](https://java.com)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.17.2-red.svg)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6.0-black.svg)](https://kafka.apache.org/)
[![Avro](https://img.shields.io/badge/Avro-1.11.3-green.svg)](https://avro.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
## 📌 Overview
A production-ready **real-time AdTech streaming pipeline** capable of processing **1-5 million events per second** with sub-second latency. Built for ad impression analysis, user behavior tracking, and real-time ad decisioning.
### 🎯 Key Features
- ✅ **Real-time Processing** - Process millions of events with sub-second latency
- ✅ **Exactly-Once Semantics** - No data loss or duplication
- ✅ **Automatic Scaling** - 10 partitions with 10 parallel subtasks
- ✅ **Comprehensive Monitoring** - Prometheus + Grafana dashboards
- ✅ **Avro Serialization** - Schema evolution support
- ✅ **Consumer Lag Alerts** - Real-time backlog monitoring
## 🏗️ Architecture
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ Kafka Producer │────▶│ Kafka Topic │────▶│ Flink Job │
│ (Ad Impressions)│ │ (10 partitions) │ │ (10 parallelism)│
└─────────────────┘ └─────────────────┘ └────────┬────────┘
│
┌─────────────────────────────────────┼─────────────────────────────────────┐
│ │ │
▼ ▼ ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ Total Count │ │ Country-wise │ │ Bid Price │
│ (10 sec window)│ │ Aggregations │ │ Statistics │
└─────────────────┘ └─────────────────┘ └─────────────────┘
│ │ │
└─────────────────────────────────────┼─────────────────────────────────────┘
▼
┌─────────────────┐
│ Grafana │
│ Dashboards │
└─────────────────┘

## 📊 Data Flow
1. **Producer** sends Avro-encoded ad impressions to Kafka
2. **Kafka** distributes messages across 10 partitions
3. **Flink** consumes with 10 parallel subtasks
4. **Real-time aggregations** every 10 seconds:
   - Total message count
   - Country-wise user distribution
   - Processing rate (msg/sec)
5. **Metrics** exported to Prometheus
6. **Visualized** in Grafana dashboards
## 🚀 Quick Start
### Prerequisites
# Required
- Docker & Docker Compose
- Java 11 or higher
- Maven 3.6+
- Kafka CLI tools (optional)
1. Clone Repository

git clone https://github.com/sattyamghumare/kafka-flink-adtech-pl.git
cd kafka-flink-adtech-pl
2. Start Kafka & Monitoring Stack

# Start all services
docker-compose -f docker-compose.monitor.yml up -d
# Check if running
docker ps | grep -E "kafka|prometheus|grafana"
3. Create Kafka Topic

# Create topic with 10 partitions
docker exec -it kappa-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic ad-impressions-avro \
  --partitions 10 \
  --replication-factor 1
  
4. Build the Project
# Clean and package
mvn clean package

# Or run directly in IntelliJ IDEA
5. Run the Producer
# Run producer (sends sample ad impressions)
java -jar target/kafka-avro-producer-1.0.jar


# Right-click on AvroProducer.java -> Run
6. Run Flink Consumer

# Run consumer (processes real-time data)
java -cp target/kafka-avro-producer-1.0.jar org.adtech.consumer.FlinkAvroConsumer

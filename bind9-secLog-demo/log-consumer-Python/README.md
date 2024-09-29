# Kafka Log Processor

This project contains two versions of a Kafka log processor:

1. Standard version using confluent-kafka
2. Distributed version using Apache Spark

## Requirements

- Python 3.7+
- Kafka cluster
- (For Spark version) Apache Spark 3.1.2

## Installation

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. For the Spark version, ensure you have Apache Spark 3.1.2 installed and configured

## Usage

### Standard Version

Run the standard version with:

```
python kafka_log_processor.py
```

### Spark Version

Run the Spark version with:

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_log_processor_spark.py
```

## Configuration

- Update Kafka configuration in both files as needed
- For the Spark version, adjust Spark configurations in `kafka_log_processor_spark.py` as necessary

## Output

- The standard version saves results to a JSON file
- The Spark version writes results to the 'output' directory in JSON format
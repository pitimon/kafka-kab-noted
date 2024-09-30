from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_extract, count, when, from_unixtime, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
import datetime
import os

def load_properties(filename):
    properties = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key_value = line.split('=', 1)
                if len(key_value) == 2:
                    key, value = key_value
                    key = key.strip()
                    value = value.strip()
                    
                    # จัดการกับค่าที่มีเครื่องหมาย = ในตัวมัน
                    if key == 'sasl.jaas.config':
                        value = line.split('=', 1)[1].strip()
                    
                    # แปลงพาธของ truststore เป็นพาธแบบสัมบูรณ์
                    if key == 'ssl.truststore.location':
                        value = os.path.abspath(value)
                    
                    properties[key] = value
    
    return properties

def create_spark_session(properties):
    return SparkSession.builder \
        .appName("KafkaLogProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.driver.extraJavaOptions", "-Djavax.net.debug=ssl,handshake -Djava.security.debug=all") \
        .config("spark.executor.extraJavaOptions", "-Djavax.net.debug=ssl,handshake -Djava.security.debug=all") \
        .config("spark.kafka.bootstrap.servers", properties['bootstrap.servers']) \
        .config("spark.kafka.sasl.jaas.config", properties['sasl.jaas.config']) \
        .config("spark.kafka.security.protocol", properties['security.protocol']) \
        .config("spark.kafka.sasl.mechanism", properties['sasl.mechanism']) \
        .config("spark.kafka.ssl.truststore.location", properties['ssl.truststore.location']) \
        .config("spark.kafka.ssl.truststore.password", properties['ssl.truststore.password']) \
        .config("spark.kafka.ssl.endpoint.identification.algorithm", properties['ssl.endpoint.identification.algorithm']) \
        .config("spark.kafka.ssl.protocol", properties['ssl.protocol']) \
        .config("spark.kafka.ssl.enabled.protocols", properties['ssl.enabled.protocols']) \
        .config("spark.driver.extraJavaOptions", "-Djavax.net.debug=ssl,handshake") \
        .getOrCreate()

def write_batch(df, epoch_id, output_path):
    now = datetime.datetime.now()
    output_file = f"{output_path}/results_{now.strftime('%Y%m%d_%H%M%S')}.json"
    df.coalesce(1).write.json(output_file, mode="overwrite")

def process_logs_with_spark():
    properties = load_properties('k0100-client.properties')
    spark = create_spark_session(properties)

    schema = StructType([
        StructField("file_name", StringType()),
        StructField("content", StringType()),
        StructField("timestamp", DoubleType())
    ])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", properties['bootstrap.servers']) \
        .option("subscribe", "logCentral") \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", properties['security.protocol']) \
        .option("kafka.sasl.mechanism", properties['sasl.mechanism']) \
        .option("kafka.sasl.jaas.config", properties['sasl.jaas.config']) \
        .option("kafka.ssl.truststore.type", "JKS")  \
        .option("kafka.ssl.truststore.location", properties['ssl.truststore.location']) \
        .option("kafka.ssl.truststore.password", properties['ssl.truststore.password']) \
        .option("kafka.ssl.endpoint.identification.algorithm", properties['ssl.endpoint.identification.algorithm']) \
        .option("kafka.ssl.protocol", properties['ssl.protocol']) \
        .option("kafka.ssl.enabled.protocols", properties['ssl.enabled.protocols']) \
        .load()

    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    denied_logs = parsed_df.filter(
        (col("file_name") == "security.log") & 
        (col("content").contains("denied"))
    )

    processed_logs = denied_logs.select(
        regexp_extract(col("content"), r"(\d+\.\d+\.\d+\.\d+)", 1).alias("ip"),
        regexp_extract(col("content"), r"\((.*?)\)", 1).alias("domain"),
        from_unixtime(col("timestamp")).alias("datetime")
    )

    # IP aggregation
    ip_counts = processed_logs.groupBy("ip") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(20)

    # Domain aggregation
    domain_counts = processed_logs.groupBy("domain") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(20)

    # Summary statistics
    summary = processed_logs.agg(
        count("*").alias("total_processed"),
        count("ip").alias("total_unique_ips"),
        count("domain").alias("total_unique_domains")
    )

    # Write IP results
    query_ip_results = ip_counts \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: write_batch(df, epoch_id, "output/ip_results")) \
        .outputMode("complete") \
        .trigger(processingTime="1 minute") \
        .start()

    # Write domain results
    query_domain_results = domain_counts \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: write_batch(df, epoch_id, "output/domain_results")) \
        .outputMode("complete") \
        .trigger(processingTime="1 minute") \
        .start()

    # Write summary
    query_summary = summary \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: write_batch(df, epoch_id, "output/summary")) \
        .outputMode("complete") \
        .trigger(processingTime="1 minute") \
        .start()
    
    # เพิ่ม query สำหรับแสดงผลบน console
    query_console = processed_logs \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # รอให้ query ใดๆ จบการทำงาน
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    process_logs_with_spark()
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()

# Define Kafka consumer configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "xml_topic"

# Read streaming data from Kafka
kafka_stream_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Deserialize JSON data from the 'value' column
deserialized_data = kafka_stream_data.selectExpr(
    "CAST(value AS STRING) AS value"
)

# Define HDFS Parquet sink path
hdfs_parquet_path = "hdfs:///your/hdfs/path"

# Write the streaming data to HDFS Parquet table
hdfs_parquet_query = deserialized_data.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", hdfs_parquet_path) \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()

# Wait for the stream to finish
hdfs_parquet_query.awaitTermination()

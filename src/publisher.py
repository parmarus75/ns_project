from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import to_xml

# Initialize Spark Session
spark = SparkSession.builder.appName("S3ToKafkaDataPublisher").getOrCreate()

# Define schema for the nested XML data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("author", StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True)
    ]), True),
    StructField("title", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("publish_date", DateType(), True),
    StructField("description", StructType([
        StructField("summary", StringType(), True),
        StructField("details", StringType(), True)
    ]), True)
])

# Read streaming data from S3
s3_data = spark.readStream \
    .format("xml") \
    .option("path", "s3://your_bucket/your_path") \
    .option("schema", schema.json()) \
    .load()

# Define Kafka producer configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "xml_topic"

# Write data to Kafka topic as a stream
stream_query = s3_data \
    .selectExpr("CAST(id AS STRING) AS key", "CAST(to_xml(struct(*))) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", kafka_topic) \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()

# Wait for the stream to finish
stream_query.awaitTermination()

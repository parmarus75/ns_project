from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Initialize Spark Session
# No need to build or create session in databricks.
spark = SparkSession.builder.appName("HDFSStreamingToDelta").getOrCreate()

# Define HDFS Parquet RAW Zone path
hdfs_raw_path = "hdfs:///your/hdfs/path"

# Define Delta table path in Databricks
delta_table_path = "catalog.xml_raw_schema.xml_raw"

spark.sql(f"create schema catalog.xml_raw_schema if not exists")

# Define HDFS Parquet streaming source
streaming_data = spark.readStream \
    .format("text") \
    .load(hdfs_raw_path)

# Write the raw XML data to Delta table with continuous trigger
delta_query = streaming_data.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .trigger(continuous="10 second") \
    .start("delta.`{}`".format(delta_table_path))


# Wait for the stream to finish
delta_query.awaitTermination()

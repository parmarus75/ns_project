Spark Streaming to Kafka, HDFS Parquet, and Databricks
This project showcases a data processing pipeline using Apache Spark for streaming data to Kafka, reading data into HDFS in Parquet format, loading data into Databricks, and performing data cleaning for silver and gold layers.

Usage
1. Streaming to Kafka
Run the Spark Streaming script to read streaming data from a directory and write it to a Kafka topic.
2. Reading to HDFS Parquet
Read data from Kafka, process it, and write the transformed data to HDFS in Parquet format.
3. Loading to Databricks
Load the Parquet data into Databricks for further analysis. Execute the notebook or script in Databricks.
4. Cleaning Data to Silver and Gold Layer
Process the loaded data, creating silver and gold layers for downstream analytics. Execute the notebook or script in Databricks.

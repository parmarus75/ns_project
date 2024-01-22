import dlt
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import col
from pyspark.sql.functions import expr

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


@dlt.table(
  comment="cleaned xml data"
  name = "cleaned_xml_table"
)
def clickstream_raw():
  df = spark.read.format("xml").table("catalog.xml_raw_schema.xml_raw").schema(schema)
  df = df.withColumn["xml_column"].parallelize()
  flattened_df = df.select(
    col("id"),
    col("xml_column.book.id").alias("book_id"),
    col("xml_column.book.author.first_name").alias("author_first_name"),
    col("xml_column.book.author.last_name").alias("author_last_name"),
    col("xml_column.book.title").alias("title"),
    col("xml_column.book.genre").alias("genre"),
    col("xml_column.book.price").alias("price"),
    col("xml_column.book.publish_date").alias("publish_date"),
    col("xml_column.book.description.summary").alias("description_summary"),
    col("xml_column.book.description.details").alias("description_details")
)
  return df


# final table and quarentine table
rules = {}
rules["valid_id"] = "(id IS NOT NULL)"
rules["book_id"] = "(book_id IS NOT NULL)"
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))


@dlt.table(
  name="pre_gold_xml",
  temporary=True,
  partition_cols=["is_quarantined"]
)
@dlt.expect_all(rules)
def pre_gold_xml():
  return (
    dlt.readStream("quarentine_xml_table")
      .select("id", "book_id", "author_first_name", "author_last_name",
              "title", "genre", "price", "publish_date", "description_summary", "description_details")
      .withColumn("is_quarantined", expr(quarantine_rules))
  )

@dlt.view(
  name="valid_gold_xml"
)
def get_valid_farmers_market():
  return (
    dlt.read("pre_gold_xml")
      .filter("is_quarantined=false")
  )

@dlt.view(
  name="invalid_gold_xml"
)
def get_invalid_farmers_market():
  return (
    dlt.read("pre_gold_xml")
      .filter("is_quarantined=true")
  )


# create final table

dlt.create_streaming_table("gold_xml")

dlt.apply_changes(
  target = "target",
  source = "valid_gold_xml",
  keys = ["id"],
  sequence_by = col("publish_date"),
  stored_as_scd_type = 1
)

# final quarentine table



@dlt.table(
  name="gold_xml_quarentine",
)
def gold_quarentine():
  return (
    dlt.readStream("invalid_gold_xml")
  )










import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Netflix-Revenue-Pipeline") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "hadoop") \
        .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # 2. Define Schema (Matching Producer)
    schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("country", StringType(), True),
        StructField("timestamp", DoubleType(), True) # Read as Double first
    ])

    # 3. Read from Redpanda
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "redpanda:29092") \
        .option("subscribe", "subscription_events") \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. Parse & Transform (THE FIX IS HERE)
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Cast the double (unix time) to a Timestamp object so Iceberg accepts it
    df_transformed = df_parsed.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # 5. Write to Iceberg
    query = df_transformed.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("path", "demo.streamflix.revenue_table") \
        .option("checkpointLocation", "/home/jovyan/work/checkpoints/netflix_revenue_v7") \
        .start()

    logger.info("Streaming Query Started...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
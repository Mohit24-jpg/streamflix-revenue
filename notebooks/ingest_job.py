from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark (SAME configs as Setup)
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

# 2. Schema
json_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

# 3. Read Stream
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:29092") \
    .option("subscribe", "subscription_events") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transform
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")

# 5. Write to Iceberg (Targeting the NEW demo catalog)
query = df_parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("path", "demo.streamflix.revenue_table") \
    .option("checkpointLocation", "/home/jovyan/work/checkpoints/netflix_revenue_v5") \
    .start()

query.awaitTermination()
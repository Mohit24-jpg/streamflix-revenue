from pyspark.sql import SparkSession

# 1. Initialize Spark (FORCE In-Memory Metadata to kill Hive errors)
spark = SparkSession.builder \
    .appName("Netflix-Table-Setup") \
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

# 2. Create the Database in our CUSTOM catalog
print("Creating Namespace 'demo.streamflix'...")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.streamflix")

# 3. Create the Table
print("Creating Iceberg Table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.streamflix.revenue_table (
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        country STRING,
        timestamp DOUBLE
    ) USING iceberg
""")

print("SUCCESS: Table 'demo.streamflix.revenue_table' created!")
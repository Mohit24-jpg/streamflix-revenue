from pyspark.sql import SparkSession

# 1. Initialize Spark (Same Configs as Ingest)
spark = SparkSession.builder \
    .appName("Streamflix-Reader") \
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

print("\n--- QUERYING DATA LAKE ---")

# 2. Run SQL Query
# We can use standard SQL because Iceberg makes files look like tables
df = spark.sql("""
    SELECT * FROM demo.streamflix.revenue_table 
    ORDER BY timestamp DESC 
    LIMIT 20
""")

# 3. Show Results
df.show(truncate=False)

# 4. Count Total Rows
count = spark.sql("SELECT COUNT(*) FROM demo.streamflix.revenue_table").collect()[0][0]
print(f"\nTotal Transactions Captured: {count}")
print("--------------------------\n")
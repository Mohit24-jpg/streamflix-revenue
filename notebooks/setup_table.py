import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_bucket():
    """
    Creates the 'warehouse' bucket in MinIO if it doesn't exist.
    """
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password'
    )
    
    try:
        s3.create_bucket(Bucket='warehouse')
        print("✅ Bucket 'warehouse' created successfully.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print("✅ Bucket 'warehouse' already exists.")
    except Exception as e:
        print(f"❌ Error creating bucket: {e}")

def create_table():
    """
    Creates the Iceberg table schema in the Data Lake.
    """
    spark = SparkSession.builder \
        .appName("Streamflix-Setup") \
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

    print("\n--- INITIALIZING DATA LAKE ---")
    
    # 1. Create Database (Namespace)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.streamflix")
    
    # 2. Create Table
    # ACID Transactions enabled by default in Iceberg
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.streamflix.revenue_table (
            event_type STRING,
            user_id STRING,
            amount DOUBLE,
            country STRING,
            timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (country)
    """)
    
    print("✅ Iceberg Table 'demo.streamflix.revenue_table' created successfully.")
    print("------------------------------\n")

if __name__ == "__main__":
    # 1. Install boto3 if missing (Running inside Docker)
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3"])
    
    # 2. Run Setup
    create_bucket()
    create_table()
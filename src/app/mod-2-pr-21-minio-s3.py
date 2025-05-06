"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-21-minio-s3.py

# =======================================================
# Spark Integration with MinIO/S3
# =======================================================

To integrate Spark with MinIO/S3, the following JAR files are required:

1. hadoop-aws.jar:
   - Contains the S3A filesystem implementation (org.apache.hadoop.fs.s3a.S3AFileSystem)
   - Version should match your Hadoop version
   - Example: hadoop-aws-3.3.1.jar for Hadoop 3.3.1

2. aws-java-sdk-bundle.jar:
   - Contains the AWS SDK libraries used by hadoop-aws
   - Version must be compatible with your hadoop-aws version
   - Example: aws-java-sdk-bundle-1.11.901.jar

3. Optional format-specific JARs:
   - For Avro: spark-avro.jar
   - For ORC: orc-core.jar and orc-mapreduce.jar

## CONFIGURATION CATEGORIES

### 1. Basic Connection Configuration
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # S3A filesystem implementation
.config("spark.hadoop.fs.s3a.endpoint", "http://24.144.65.249:80") # MinIO server endpoint
.config("spark.hadoop.fs.s3a.access.key", "miniolake") # Access key
.config("spark.hadoop.fs.s3a.secret.key", "LakE142536@@") # Secret key
.config("spark.hadoop.fs.s3a.path.style.access", "true") # Use path-style instead of virtual-hosted style
.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") # Disable SSL for testing environments

### 2. Connection Optimizations
.config("spark.hadoop.fs.s3a.connection.maximum", "100") # Connection pool size
.config("spark.hadoop.fs.s3a.threads.max", "20") # Maximum thread count for parallel operations
.config("spark.hadoop.fs.s3a.connection.timeout", "300000") # Connection timeout in milliseconds (5 minutes)
.config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") # Timeout to establish connection (5 seconds)

### 3. Read Optimizations
.config("spark.hadoop.fs.s3a.readahead.range", "256K") # Readahead buffer size

### 4. Write Optimizations
.config("spark.hadoop.fs.s3a.fast.upload", "true") # Enable fast upload path
.config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") # Buffer type (memory, disk, array)
.config("spark.hadoop.fs.s3a.multipart.size", "64M") # Size of each multipart chunk
.config("spark.hadoop.fs.s3a.multipart.threshold", "64M") # Threshold to trigger multipart upload

### 5. Committer Optimizations
.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") # Faster commit algorithm
.config("spark.hadoop.fs.s3a.committer.name", "directory") # Committer type

### 6. Parquet Optimizations
.config("spark.sql.parquet.filterPushdown", "true") # Enable pushdown of filters to Parquet
.config("spark.sql.parquet.mergeSchema", "false") # Disable schema merging (improves performance)
.config("spark.sql.parquet.columnarReaderBatchSize", "4096") # Batch size for columnar reader

### 7. Adaptive Query Execution
```python
.config("spark.sql.adaptive.enabled", "true") # Enable adaptive query execution
.config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Enable partition coalescing
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, count, avg, sum, min, max, stddev, percentile_approx
from pyspark.sql.functions import year, month, dayofmonth, hour, date_format, to_timestamp
from pyspark.sql.functions import when, expr, datediff, current_date, lit, unix_timestamp
from pyspark.sql.window import Window
import time

# TODO Initialize Spark session
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://24.144.65.249:80") \
    .config("spark.hadoop.fs.s3a.access.key", "miniolake") \
    .config("spark.hadoop.fs.s3a.secret.key", "LakE142536@@") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.threads.max", "20") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "300000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.readahead.range", "256K") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
    .config("spark.hadoop.fs.s3a.multipart.size", "64M") \
    .config("spark.hadoop.fs.s3a.multipart.threshold", "64M") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.fs.s3a.committer.name", "directory") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# TODO configs
spark.sparkContext.setLogLevel("ERROR")

# TODO define schema
ratings_schema = StructType([
    StructField("rating_id", IntegerType(), True),
    StructField("uuid", StringType(), True),
    StructField("restaurant_identifier", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("dt_current_timestamp", TimestampType(), True)
])


# TODO Stop Spark session
spark.stop()

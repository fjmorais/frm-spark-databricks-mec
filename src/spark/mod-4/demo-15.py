"""
Apache Iceberg Demo 9: Advanced Query Optimization
===================================================

This demo covers:
- Apache Iceberg: Data Skipping with Statistics
- Apache Iceberg: Bloom Filters
- Apache Iceberg: Spark Adaptive Query Execution (AQE) + Iceberg: Auto-Optimization
- Apache Iceberg: Predicate Pushdown
- Apache Iceberg: Column Pruning
- Apache Iceberg: Dynamic File Pruning
- Apache Iceberg: Vectorized Reads

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-15.py
"""

import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def spark_session():
    """Create Spark Session with Apache Iceberg and MinIO support"""

    encoded_access_key = "bWluaW9sYWtl"
    encoded_secret_key = "TGFrRTE0MjUzNkBA"
    access_key = base64.b64decode(encoded_access_key).decode("utf-8")
    secret_key = base64.b64decode(encoded_secret_key).decode("utf-8")

    spark = SparkSession.builder \
        .appName("IcebergDemo9-QueryOptimization") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3a://owshq-catalog/warehouse") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://24.144.65.249:80") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.iceberg.vectorization.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✅ Apache Iceberg Spark Session Created Successfully!")
    print(f"🚀 Spark Version: {spark.version}")

    return spark

def setup_namespace(spark):
    """Setup namespace for demo"""

    print("\n=== Setting Up Demo Namespace ===")

    # TODO create namespace
    print("📁 creating namespace...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.ubereats_demo9")

    # TODO set catalog context
    spark.catalog.setCurrentCatalog("hadoop_catalog")
    spark.catalog.setCurrentDatabase("ubereats_demo9")

    print("✅ namespace ready!")

def data_skipping_with_statistics(spark):
    """Demonstrate Data Skipping with Statistics"""

    print("\n=== Apache Iceberg: Data Skipping with Statistics ===")

    table_fq = "hadoop_catalog.ubereats_demo9.orders"

    # TODO create table with range-based data
    print("🏗️ creating table...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  amount DOUBLE,
                  status STRING
              ) USING iceberg
              """)

    # TODO insert data with different ranges
    print("💾 inserting data with different amount ranges...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 10.00, 'completed'),
              ('ORD-002', 15.00, 'pending')
              """)

    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-003', 100.00, 'completed'),
              ('ORD-004', 150.00, 'processing')
              """)

    # TODO show file statistics
    print("🔍 file statistics (min/max values)...")
    spark.sql(f"""
              SELECT file_path, lower_bounds, upper_bounds
              FROM {table_fq}.files
              """).show(truncate=False)

    # TODO demonstrate data skipping
    print("🔍 query with data skipping (amount > 50)...")
    spark.sql(f"SELECT * FROM {table_fq} WHERE amount > 50").show()

    print("✅ Data skipping: Only files with amount > 50 are read!")

def bloom_filters(spark):
    """
    Demonstrate Iceberg's automatic use of Bloom filters for fast point lookups.
    No special table property is needed—Bloom filters are enabled by default for string columns.
    """

    print("\n=== Apache Iceberg: Bloom Filters ===")

    table_fq = "hadoop_catalog.ubereats_demo9.orders_bloom"

    # 1. Create the table (no need for special properties)
    print("🏗️ Creating table (Bloom filters are automatic for string columns)...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_fq}
        (
            order_id STRING,
            amount DOUBLE,
            status STRING
        ) USING iceberg
    """)

    # 2. Insert data
    print("💾 Inserting data...")
    spark.sql(f"""
        INSERT INTO {table_fq} VALUES
        ('ORD-A001', 25.50, 'completed'),
        ('ORD-B002', 18.75, 'pending'),
        ('ORD-C003', 32.00, 'processing')
    """)

    # 3. Demonstrate point lookup (Iceberg will use Bloom filters automatically)
    print("🔍 Point lookup for order_id = 'ORD-B002' (Bloom filter in action)...")
    spark.sql(f"SELECT * FROM {table_fq} WHERE order_id = 'ORD-B002'").show()

    print("✅ Bloom filter: Fast point lookups without scanning all files! (No extra config needed)")


def spark_aqe_with_iceberg(spark):
    """Demonstrate Spark AQE + Iceberg Auto-Optimization"""

    print("\n=== Apache Iceberg: Spark AQE + Auto-Optimization ===")

    table_fq = "hadoop_catalog.ubereats_demo9.orders"

    # TODO show AQE settings
    print("🔍 AQE settings...")
    aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
    print(f"   📊 Adaptive Query Execution: {aqe_enabled}")

    # TODO demonstrate AQE with aggregation
    print("🔍 aggregation query with AQE...")
    spark.sql(f"""
              SELECT status, COUNT(*) as count, AVG(amount) as avg_amount
              FROM {table_fq}
              GROUP BY status
              """).show()

    print("✅ AQE: Automatically optimizes query execution at runtime!")

def predicate_pushdown(spark):
    """Demonstrate Predicate Pushdown"""

    print("\n=== Apache Iceberg: Predicate Pushdown ===")

    table_fq = "hadoop_catalog.ubereats_demo9.orders_partitioned"

    # TODO create partitioned table
    print("🏗️ creating partitioned table...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  amount DOUBLE,
                  status STRING
              ) USING iceberg
              PARTITIONED BY (status)
              """)

    # TODO insert data across partitions
    print("💾 inserting data across partitions...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 25.50, 'completed'),
              ('ORD-002', 18.75, 'pending'),
              ('ORD-003', 32.00, 'processing')
              """)

    # TODO show all partitions
    print("🔍 all partitions...")
    spark.sql(f"SELECT * FROM {table_fq}.partitions").show()

    # TODO demonstrate predicate pushdown
    print("🔍 query with predicate pushdown...")
    spark.sql(f"SELECT * FROM {table_fq} WHERE status = 'completed'").show()

    print("✅ Predicate pushdown: Only 'completed' partition is read!")

def column_pruning(spark):
    """Demonstrate Column Pruning"""

    print("\n=== Apache Iceberg: Column Pruning ===")

    table_fq = "hadoop_catalog.ubereats_demo9.orders_wide"

    # TODO create table with many columns
    print("🏗️ creating table with many columns...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  amount DOUBLE,
                  tax DOUBLE,
                  tip DOUBLE,
                  total DOUBLE,
                  status STRING,
                  notes STRING
              ) USING iceberg
              """)

    # TODO insert data
    print("💾 inserting data...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 25.50, 2.55, 3.00, 31.05, 'completed', 'fast delivery'),
              ('ORD-002', 18.75, 1.88, 2.50, 23.13, 'pending', 'no onions')
              """)

    # TODO demonstrate column pruning
    print("🔍 selecting only needed columns...")
    spark.sql(f"SELECT order_id, amount, status FROM {table_fq}").show()

    print("✅ Column pruning: Only 3 columns read instead of 7!")

def dynamic_file_pruning(spark):
    """Demonstrate Dynamic File Pruning"""

    print("\n=== Apache Iceberg: Dynamic File Pruning ===")

    table_fq = "hadoop_catalog.ubereats_demo9.orders_partitioned"

    # TODO show total files
    print("🔍 total files in table...")
    spark.sql(f"SELECT COUNT(*) as total_files FROM {table_fq}.files").show()

    # TODO demonstrate dynamic pruning
    print("🔍 query with dynamic file pruning...")
    spark.sql(f"SELECT * FROM {table_fq} WHERE status IN ('completed', 'pending')").show()

    print("✅ Dynamic pruning: Only relevant files are accessed!")

def vectorized_reads(spark):
    """Demonstrate Vectorized Reads"""

    print("\n=== Apache Iceberg: Vectorized Reads ===")

    table_fq = "hadoop_catalog.ubereats_demo9.orders"

    # TODO show vectorization setting
    print("🔍 vectorization setting...")
    vectorized = spark.conf.get("spark.sql.iceberg.vectorization.enabled")
    print(f"   📊 Vectorized reads: {vectorized}")

    # TODO demonstrate vectorized operations
    print("🔍 vectorized aggregation...")
    spark.sql(f"""
              SELECT 
                  COUNT(*) as total_orders,
                  SUM(amount) as total_amount,
                  AVG(amount) as avg_amount
              FROM {table_fq}
              """).show()

    print("✅ Vectorized reads: Process data in batches for better performance!")

def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables
        tables = [
            'hadoop_catalog.ubereats_demo9.orders',
            'hadoop_catalog.ubereats_demo9.orders_bloom',
            'hadoop_catalog.ubereats_demo9.orders_partitioned',
            'hadoop_catalog.ubereats_demo9.orders_wide'
        ]

        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")

        # TODO drop namespace
        spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.ubereats_demo9 CASCADE")

        print("✅ demo resources cleaned up successfully!")

    except Exception as e:
        print(f"⚠️ cleanup warning: {e}")

def main():
    """Main demo execution"""

    print("🚀 Starting Apache Iceberg Demo 9: Advanced Query Optimization")
    print("=" * 70)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        setup_namespace(spark)
        data_skipping_with_statistics(spark)
        bloom_filters(spark)
        spark_aqe_with_iceberg(spark)
        predicate_pushdown(spark)
        column_pruning(spark)
        dynamic_file_pruning(spark)
        vectorized_reads(spark)

        print("\n" + "=" * 70)
        print("🎉 Demo 9 completed successfully!")
        print("📚 Key insights:")
        print("   💡 Data skipping: Skip files using min/max statistics")
        print("   💡 Bloom filters: Fast point lookups")
        print("   💡 AQE: Automatic runtime optimization")
        print("   💡 Predicate pushdown: Filter at storage level")
        print("   💡 Column pruning: Read only needed columns")
        print("   💡 Dynamic pruning: Skip files at runtime")
        print("   💡 Vectorized reads: Process data in batches")

        print("\n🚀 Result: Dramatically faster queries!")

    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # TODO cleanup
        # cleanup_resources(spark)
        spark.stop()
        print("🔒 Spark session stopped")

if __name__ == "__main__":
    main()

"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2/mod-2-pr-2-ubereats_analysis.py
"""

# Simple file analysis script for UberEats data using PySpark

# ubereats_analysis.py
from pyspark.sql import SparkSession

def create_optimized_session():
    """Create an optimized session for UberEats data analysis."""
    return SparkSession.builder \
        .appName("UberEatsAnalysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# Create session
spark = create_optimized_session()

try:
    # Load data from storage directory
    restaurants = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
    ratings = spark.read.json("./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl")
    
    # Register temporary views
    restaurants.createOrReplaceTempView("restaurants")
    ratings.createOrReplaceTempView("ratings")
    
    # Join data and analyze
    results = spark.sql("""
        SELECT 
            r.name as restaurant_name,
            r.cuisine_type,
            r.city,
            r.average_rating,
            r.num_reviews
        FROM restaurants r
        WHERE r.average_rating > 4.0
        ORDER BY r.average_rating DESC, r.num_reviews DESC
        LIMIT 10
    """)
    
    # Show results
    print("===== Top Rated Restaurants =====")
    results.show(truncate=False)
    
    # Output session info
    sc = spark.sparkContext
    print(f"\nSpark Version: {spark.version}")
    print(f"Application ID: {sc.applicationId}")
    print(f"Number of configured executors: {sc.getConf().get('spark.executor.instances', '1')}")
    
finally:
    # Always stop the session
    spark.stop()
    print("\nSparkSession stopped")
"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2/mod-2-pr-2-analysis_job.py
"""

# This script analyzes restaurant data from UberEats using Apache Spark.
# We create a file named spark_factory.py to handle Spark session management.
# The analysis includes counting restaurants by cuisine type and calculating average ratings.
# The spark factory has class methods to get or create a Spark session, load configurations from a YAML file, and run jobs with proper session management.

# analysis_job.py
import os
import logging
from spark_factory import SparkFactory

# Set up logging
logging.basicConfig(level=logging.INFO)

def analyze_restaurants(spark):
    """Analyze restaurant data from UberEats."""
    # Load data from storage directory
    restaurants = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
    
    # Register as temp view for SQL
    restaurants.createOrReplaceTempView("restaurants")
    
    # Perform analysis
    result = spark.sql("""
        SELECT 
            cuisine_type, 
            COUNT(*) as count,
            ROUND(AVG(average_rating), 2) as avg_rating
        FROM restaurants
        GROUP BY cuisine_type
        ORDER BY avg_rating DESC
    """)
    
    return result

if __name__ == "__main__":
    # Set environment
    os.environ["SPARK_ENV"] = "dev"  # "test" or "prod" in other environments
    
    # Run the analysis
    result = SparkFactory.run_job(analyze_restaurants, "RestaurantAnalysis")
    
    # Display results
    result.show()
    
    # Clean up
    SparkFactory.stop_session()


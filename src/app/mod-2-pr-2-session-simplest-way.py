"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-2-session-simplest-way.py
"""

from pyspark.sql import SparkSession

# Basic configuration
spark = SparkSession.builder \
    .appName("UberEatsAnalytics") \
    .master("local[*]") \
    .getOrCreate()

# Load data from the repository storage
users_df = spark.read.json("./storage/mongodb/users/01JS4W5A7WWZBQ6Y1C465EYR76.jsonl")
users_df.show()

# Always stop the session when done
spark.stop()
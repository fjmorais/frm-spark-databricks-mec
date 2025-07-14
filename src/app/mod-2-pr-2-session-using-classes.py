"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-2-session-using-classes.py
"""

from spark_manager import SparkManager

# Get the singleton session
spark = SparkManager.get_session()

# Use it for data processing
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
restaurants_df.show(5)

# Stop when done
SparkManager.stop_session()
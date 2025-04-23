"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-6-complex-transformation.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, max, min, round, desc
from pyspark.sql.functions import concat, lit, when, expr

spark = SparkSession.builder \
    .getOrCreate()



spark.stop()

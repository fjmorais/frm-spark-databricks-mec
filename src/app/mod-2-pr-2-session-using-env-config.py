"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-2-session-using-env-config.py
"""

import os
from env_config import create_session

# Set environment
os.environ["SPARK_ENV"] = "dev"  # or "test" or "prod"

# Create environment-specific session
spark = create_session()

# Process data
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
print(f"===================================")
print(f"===================================")
print(f"===================================")
print(f"===================================")
print(f"Drivers count: {drivers_df.count()}")
print(f"===================================")
print(f"===================================")
print(f"===================================")
print(f"===================================")

# Clean up
spark.stop()
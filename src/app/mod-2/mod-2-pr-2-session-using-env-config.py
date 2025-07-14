"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2/mod-2-pr-2-session-using-env-config.py
"""

# this script is used to create a Spark session based on the environment configuration
# the environment can be set to "dev", "test", or "prod" and we use the env_config module to create the session accordingly
# we create the env_config.py file to acomplish this
# the env_config.py file should contain the logic to create a Spark session based on the environment

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
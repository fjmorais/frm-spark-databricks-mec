# env_config.py
import os
from pyspark.sql import SparkSession

def create_session():
    """Create a session based on environment"""
    env = os.environ.get("SPARK_ENV", "dev")
    print(f"Creating session for: {env}")
    
    # Start building the session
    builder = SparkSession.builder.appName(f"UberEats-{env}")
    
    # Apply environment-specific configs
    if env == "dev":
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "10")
    elif env == "test":
        builder = builder \
            .master("yarn") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.memory", "4g")
    elif env == "prod":
        builder = builder \
            .master("yarn") \
            .config("spark.submit.deployMode", "cluster") \
            .config("spark.executor.instances", "5") \
            .config("spark.executor.memory", "8g")
    
    return builder.getOrCreate()
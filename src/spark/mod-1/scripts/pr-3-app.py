from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("pr-3-app") \
    .getOrCreate()

df_users = spark.read.json("data/users.json")
count = df_users.count()
df_users.show(3)

spark.stop()

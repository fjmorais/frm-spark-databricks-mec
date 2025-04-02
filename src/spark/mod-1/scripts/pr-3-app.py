from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Processamento de Usuários") \
    .getOrCreate()

print("Carregando dados de usuários...")
usuarios_df = spark.read.json("data/mongodb_users.json")

count = usuarios_df.count()
print(f"Total de usuários: {count}")

print("\nPrimeiros registros:")
usuarios_df.show(3)

spark.stop()

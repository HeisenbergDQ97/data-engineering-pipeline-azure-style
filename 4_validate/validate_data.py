import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PARQUET = os.path.join(BASE_DIR, "../data/processed_data")

# Iniciar Spark
spark = SparkSession.builder.appName("ValidateData").getOrCreate()
print(" Sesión de Spark iniciada correctamente")

# Cargar datos
df = spark.read.parquet(INPUT_PARQUET)
print(f" Datos cargados desde: {INPUT_PARQUET}")

# Mostrar top 5 filas
df.show(5, truncate=False)

# Mostrar esquema
df.printSchema()

# Contar total de filas
print(f" Total de filas: {df.count()}")

# Contar valores nulos por columna
print(" Valores nulos por columna:")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Cerrar Spark
spark.stop()
print(" Sesión de Spark finalizada correctamente")
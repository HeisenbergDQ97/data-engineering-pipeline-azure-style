import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# 1️ Crear la sesion de Spark
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .getOrCreate()

print(" Sesión de Spark iniciada correctamente")

# 2 Leer los datos extraidos
input_path = "C:/Users/heise/data-engineering-pipeline-azure-style/data/sample_data.csv"
df = spark.read.option("header", True).csv(input_path)

print(f" Datos cargados desde: {input_path}")
df.show(5)

# 3️ Seleccionar y limpiar columnas
df_clean = df.select(
    col("id").alias("user_id"),
    col("name"),
    col("username"),
    col("email"),
    col("phone")
)

# 4️ Eliminar duplicados (si los hay)
df_clean = df_clean.dropDuplicates(["user_id"])

# 5️ Guardar los datos procesados en formato Parquet
output_path = "C:/Users/heise/data-engineering-pipeline-azure-style/data/processed_data"
df_clean.write.mode("overwrite").parquet(output_path)

print(f" Datos transformados y guardados en: {output_path}")

# 6️ Cerrar la sesion de Spark
spark.stop()
print(" Sesión de Spark finalizada correctamente")

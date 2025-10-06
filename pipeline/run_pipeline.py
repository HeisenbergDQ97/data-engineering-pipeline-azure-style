from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os

# -----------------------------
# Configuración de rutas
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(BASE_DIR, r"C:\Users\heise\data-engineering-pipeline-azure-style\data\sample_data.csv")
OUTPUT_PARQUET = os.path.join(BASE_DIR, r"C:\Users\heise\data-engineering-pipeline-azure-style\data\processed_data")

# -----------------------------
# Inicializar Spark
# -----------------------------
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

print("✅ Sesión de Spark iniciada correctamente")

# -----------------------------
# Leer CSV
# -----------------------------
df = spark.read.option("header", True).csv(INPUT_CSV)
print(f"✅ Datos cargados desde: {INPUT_CSV}")
df.show(5, truncate=False)

# -----------------------------
# Transformaciones (ejemplo simple)
# -----------------------------
# Puedes agregar más transformaciones según tu proyecto
df_clean = df.dropDuplicates().na.fill("N/A")
print("✅ Datos transformados")

# -----------------------------
# Guardar en Parquet
# -----------------------------
df_clean.write.mode("overwrite").parquet(OUTPUT_PARQUET)
print(f"✅ Datos guardados en Parquet: {OUTPUT_PARQUET}")

# -----------------------------
# Validación de datos
# -----------------------------
print("🔹 Esquema del DataFrame:")
df_clean.printSchema()

total_rows = df_clean.count()
print(f"🔹 Total de filas: {total_rows}")

print("🔹 Valores nulos por columna:")
df_clean.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in df_clean.columns]).show()

# -----------------------------
# Cerrar Spark
# -----------------------------
spark.stop()
print("🧹 Sesión de Spark finalizada correctamente")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# --- SparkSession ---
spark = (SparkSession.builder
         .appName("DemoSilenciosa")
         .master("local[*]")
         .config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# --- Paso 1: leer con esquema ---
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("ciudad", StringType(), True),
    StructField("salario", DoubleType(), True),
    StructField("departamento", StringType(), True),
])

df = spark.read.csv("data/data.csv", header=True, schema=schema)

# --- Paso 2: limpieza mínima ---
df_clean = (
    df
    .withColumn("nombre", F.regexp_replace(F.trim(F.col("nombre")), r"\s+", " "))
    .withColumn("ciudad", F.initcap(F.trim(F.col("ciudad"))))
    .withColumn("departamento", F.upper(F.trim(F.col("departamento"))))
    .withColumn("edad", F.when(F.col("edad") < 0, 0).otherwise(F.col("edad")))
    .withColumn("salario", F.when(F.col("salario") < 0, 0.0).otherwise(F.col("salario")))
)

df_clean = df_clean.fillna({"edad": 0, "salario": 0.0, "nombre": "Desconocido", "ciudad": "Desconocido", "departamento": "Desconocido"})

# --- Paso 3: KPIs y perfilado ---
print("== KPIs Generales ==")
df_clean.select(
    F.count("*").alias("total_registros"),
    F.countDistinct("ciudad").alias("n_ciudades"),
    F.countDistinct("departamento").alias("n_departamentos"),
    F.expr("percentile_approx(salario, 0.5)").alias("mediana_salario"),
    F.round(F.avg("edad"), 1).alias("edad_promedio")
).show(truncate=False)

print("== Estadísticas descriptivas ==")
df_clean.describe(["edad", "salario"]).show()

print("== Top 5 ciudades con más empleados ==")
df_clean.groupBy("ciudad").count().orderBy(F.desc("count")).show(5, truncate=False)

print("== Top 5 departamentos con mejor salario promedio ==")
df_clean.groupBy("departamento").agg(
    F.round(F.avg("salario"), 2).alias("salario_prom")
).orderBy(F.desc("salario_prom")).show(5, truncate=False)

print("== Outliers de salario (mayor a P90) ==")
p90 = df_clean.approxQuantile("salario", [0.9], 0.05)[0]
df_clean.filter(F.col("salario") > p90).orderBy(F.desc("salario")).show(10, truncate=False)

# --- Cerrar sesión ---
spark.stop()

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = (SparkSession.builder
         .appName("DemoSilenciosa")
         .master("local[*]")
         .config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")   # incluso puedes usar "OFF"

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("ciudad", StringType(), True),
    StructField("salario", DoubleType(), True),
    StructField("departamento", StringType(), True),
])

df = (spark.read
      .csv("data/data.csv", header=True, schema=schema))

# --- PASO 2: Limpieza y normalización ---

df_clean = (
    df
    # 1) Trim + normalización básica de texto
    .withColumn("nombre", F.regexp_replace(F.trim(F.col("nombre")), r"\s+", " "))
    .withColumn("ciudad", F.initcap(F.trim(F.col("ciudad"))))          # "madrid" -> "Madrid"
    .withColumn("departamento", F.upper(F.trim(F.col("departamento")))) # "it" -> "IT"
    # 2) Tipos seguros por si hubo filas raras
    .withColumn("edad", F.col("edad").cast("int"))
    .withColumn("salario", F.col("salario").cast("double"))
)

# 3) Rellenos conservadores de nulos (ajusta a tu criterio)
num_cols = ["edad", "salario"]
str_cols = ["nombre", "ciudad", "departamento"]

df_clean = (df_clean
    .fillna({"edad": 0, "salario": 0.0})
    .fillna({c: "Desconocido" for c in str_cols})
)

# 4) Reglas de corrección suaves (opcionales pero útiles)
#    - Edades o salarios negativos -> poner a 0, para no romper agregaciones
df_clean = (df_clean
    .withColumn("edad", F.when(F.col("edad") < 0, 0).otherwise(F.col("edad")))
    .withColumn("salario", F.when(F.col("salario") < 0, 0.0).otherwise(F.col("salario")))
)

print("== Muestra limpia ==")
df_clean.show(10, truncate=False)

# 5) Resumen de cambios (antes vs después)
print("== Resumen de normalización ==")
def cnt_nulls(df_):
    return df_.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_.columns]).collect()[0].asDict()

nulls_before = cnt_nulls(df)
nulls_after  = cnt_nulls(df_clean)

# valores distintos de ciudad/departamento antes vs después
distincts = (
    df.select(
        F.countDistinct("ciudad").alias("ciudad_dist_antes"),
        F.countDistinct("departamento").alias("depto_dist_antes"),
    ).crossJoin(
        df_clean.select(
            F.countDistinct("ciudad").alias("ciudad_dist_despues"),
            F.countDistinct("departamento").alias("depto_dist_despues"),
        )
    )
).collect()[0].asDict()

print("Nulos antes:", nulls_before)
print("Nulos después:", nulls_after)
print("Distincts:", distincts)

# 6) (Opcional) Guarda una versión limpia para siguientes pasos
(df_clean.write.mode("overwrite").parquet("output/empleados_limpio.parquet"))
print("Guardado: output/empleados_limpio.parquet")
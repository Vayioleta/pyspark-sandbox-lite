from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("DemoSilenciosa")
         .master("local[*]")
         .config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")   # incluso puedes usar "OFF"


# --- PASO 1: Leer con esquema fijo y validar ---

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

print("== Esquema ==")
df.printSchema()

print("== Primeras filas ==")
df.show(10, truncate=False)

# Conteo total de filas
total = df.count()
print(f"Filas totales: {total}")

# Conteo de nulos por columna
print("== Nulos por columna ==")
nulls = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
nulls.show(truncate=False)

# Reglas simples de sanidad (no bloquean, solo informan)
print("== Reglas de sanidad ==")
problemas = []

# id único (si no lo es, lo reportamos)
dupes_id = df.groupBy("id").count().filter("count > 1").count()
if dupes_id > 0:
    problemas.append(f"- Hay {dupes_id} id(s) duplicados.")

# rangos razonables
neg_edad = df.filter(F.col("edad") < 0).count()
if neg_edad > 0:
    problemas.append(f"- {neg_edad} fila(s) con edad negativa.")

neg_sal = df.filter(F.col("salario") < 0).count()
if neg_sal > 0:
    problemas.append(f"- {neg_sal} fila(s) con salario negativo.")

# strings vacíos
vac_nombre = df.filter((F.col("nombre").isNull()) | (F.trim("nombre") == "")).count()
if vac_nombre > 0:
    problemas.append(f"- {vac_nombre} fila(s) con nombre vacío/nulo.")

if problemas:
    print("Problemas detectados:")
    for p in problemas:
        print(p)
else:
    print("Sin problemas aparentes 👌")
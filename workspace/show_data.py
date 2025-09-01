from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("DemoSilenciosa")
         .master("local[*]")
         .config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")   # incluso puedes usar "OFF"

print("Spark session created")

df = spark.read.csv("data/data.csv", header=True, inferSchema=True)
df.show()
print("Data loaded")

# 👇 Cerrar sesión al final
spark.stop()
print("Spark session stopped")

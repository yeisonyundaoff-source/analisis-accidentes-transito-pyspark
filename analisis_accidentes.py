from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import matplotlib.pyplot as plt
import os

spark = SparkSession.builder \
 .appName("AnalisisAccidentes") \
 .master("local[*]") \
 .getOrCreate()

ruta = "/home/vboxuser/proyecto_spark_accidentes/datos/accidentes_transito_colombia.csv"

df = spark.read.csv(ruta, header=True,
inferSchema=True)

columnas = [
 "marca_vehiculo",
 "tipo_vehiculo",
 "edad_vehiculo",
 "gravedad_accidente",
 "departamento_accidente"
]

df_seleccionado = df.select(*columnas)

print("\n=== COLUMNAS SELECCIONADAS ===")
print(",".join(df_seleccionado.columns))


print("\n=== PRIMEROS 15 REGISTROS ===")
df_seleccionado.show(15,truncate=True)

print("\n=== TOTAL DE REGISTROS ===")
print(df_seleccionado.count())

print("\n=== VALORES NULOS POR COLUMNA ===")
df_seleccionado.select([
  sum(col(c).isNull().cast("int")).alias(c)
  for c in df_seleccionado.columns
]).show(truncate=False)

print("\n=== RESUMEN DE LAS EDADES DE LOS VEHICULOS ===")
df_seleccionado.select("edad_vehiculo").describe().show()

print("\n=== EDADES MENORES QUE CERO ===")
df_seleccionado.filter(
  col("edad_vehiculo") < 0
).select("edad_vehiculo").show()

print("\n=== EDADES SUPERIORES A 100 AÑOS ===")
df_seleccionado.filter(
  col("edad_vehiculo") > 100
).select(
  "marca_vehiculo",
  "tipo_vehiculo",
  "edad_vehiculo"
).show(20, truncate=False)



from pyspark.sql.functions import when

df_limpio = df_seleccionado.withColumn(
    "edad_vehiculo",
    when(col("edad_vehiculo") > 100, None)
    .otherwise(col("edad_vehiculo"))
)


print("\n=== VALORES NULOS DESPUES DE LA LIMPIEZA ===")
df_limpio.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_limpio.columns
]).show(truncate=False)

print("\n=== REGISTROS DESPUES DE LIMPIAR ==")
print(df_limpio.count())

print("\n=== MUESTRA DE LOS DATOS LIMPIOS ===")
df_limpio.show(5, truncate=False)



from pyspark.sql.functions import when, col

print("\n=== TRANSFORMACION: GRUPO DE EDAD ===")

df_transformado = df_limpio.withColumn(
    "grupo_edad",
    when(col("edad_vehiculo").isNull(), "Sin dato")
    .when(col("edad_vehiculo") <= 5, "0 a 5 años")
    .when(col("edad_vehiculo") <= 10, "6 a 10 años")
    .when(col("edad_vehiculo") <= 20, "11 a 20 años")
    .otherwise("Más de 20 años")
)

df_transformado.select("edad_vehiculo", "grupo_edad").show(10, truncate=False)

print("\n=== TOTAL DE REGISTROS TRANSFORMADOS ===")
print(df_transformado.count())


print("\n=== EDA: CANTIDAD DE  VEHICULOS POR GRUPO DE EDAD ==")
df_transformado.groupBy("grupo_edad").count().orderBy("grupo_edad").show(truncate=False)

print("\n=== EDA: ACCIDENTES POR GRAVEDAD ===")
df_transformado.groupBy("gravedad_accidente").count().orderBy(col("count").desc()).show(truncate=False)

print("\n=== EDA: VEHICULOS POR TIPO ===")
df_transformado.groupBy("tipo_vehiculo").count().orderBy(col("count").desc()).show(10, truncate=False)

print("\n==== EDA: REGISTROS POR DEPARTAMENTO ===")
df_transformado.groupBy("departamento_accidente").count().orderBy(col("count").desc()).show(10, truncate=False)


print("\n=== VISUALIZACION DE RESULTADOS BATCH ===")
os.makedirs("/home/vboxuser/proyecto_spark_accidentes/resultados", exist_ok=True)

fig, ejes = plt.subplots(2, 2, figsize=(12, 8))
datos = [
    ("grupo_edad", "Vehiculos por edad"),
    ("gravedad_accidente", "Registros por gravedad"),
    ("tipo_vehiculo", "Registros por tipo"),
    ("departamento_accidente", "Registros por departamentos")
]

for i, (columna, titulo) in enumerate(datos):
    resumen = df_transformado.groupBy(columna).count()

    if columna in ["tipo_vehiculo", "departamento_accidente"]:
        resumen = resumen.orderBy(col("count").desc()).limit(10)

    filas = resumen.collect()
    etiquetas = [str(f[columna]) for f in filas]
    valores = [f["count"] for f in filas]

    ax = ejes[i // 2, i % 2]
    ax.bar(etiquetas, valores)
    ax.set_title(titulo)
    ax.tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("/home/vboxuser/proyecto_spark_accidentes/resultados/graficos_batch.png", dpi=150
)
plt.close()
print("Grafico guardado en resultados/graficos_batch.png")





spark.stop()

# Análisis de accidentes de tránsito en Colombia con PySpark

## Descripción del proyecto

Este proyecto realiza el procesamiento por lotes (batch) de datos de accidentes de tránsito en Colombia utilizando Apache Spark y PySpark.

El objetivo es limpiar, transformar y analizar los registros para identificar características relacionadas con la edad de los vehículos, el tipo de vehículo, la gravedad de los accidentes y el departamento donde ocurren.

## Tecnologías utilizadas

- Python 3
- Apache Spark 3.5.8
- PySpark y DataFrames
- Matplotlib
- Dataset CSV de datos abiertos de Colombia

## Procesamiento realizado

1. Lectura del archivo CSV.
2. Selección de columnas relevantes.
3. Exploración inicial y conteo de registros.
4. Identificación y tratamiento de valores inválidos en la edad de los vehículos.
5. Creación de grupos de edad de los vehículos.
6. Análisis por edad, gravedad del accidente, tipo de vehículo y departamento.
7. Generación de gráficos con los resultados.

## Instrucciones de ejecución

1. Instalar y configurar Python, Java, Apache Spark y las dependencias necesarias.
2. Descargar el dataset y guardarlo en la carpeta `datos/` con el nombre `accidentes_transito_colombia.csv`.
3. Desde la carpeta principal del proyecto, ejecutar:

   `python3 analisis_accidentes.py`

4. Consultar los resultados en la terminal y el gráfico generado en `resultados/graficos_batch.png`.

## Estructura del proyecto

- `analisis_accidentes.py`: código fuente del procesamiento batch.
- `datos/`: contiene el archivo CSV utilizado.
- `resultados/`: contiene los gráficos generados.

## Tipo de procesamiento

Se utiliza procesamiento batch para realizar operaciones de limpieza, transformación y análisis exploratorio de datos.

## Autor
YEISON ESTIBEN YUNDA

Estudiante de Ingeniería de Sistemas.

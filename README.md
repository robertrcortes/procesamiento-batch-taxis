# Procesamiento batch de viajes de taxi con Apache Spark

## Descripción

Este repositorio contiene una solución de procesamiento **batch (por lotes)** para analizar registros históricos de viajes de taxi amarillo de la ciudad de Nueva York publicados por la New York City Taxi and Limousine Commission (NYC TLC). La implementación utiliza **Python y Apache Spark con PySpark** para leer un archivo Parquet, validar y limpiar los registros y generar indicadores agregados.

El programa procesa el archivo completo en una ejecución por lotes; no utiliza Kafka ni Spark Structured Streaming. La parte de streaming, si corresponde a la actividad, debe documentarse como un componente separado.

## Objetivos

* Leer los registros desde un archivo Parquet.
* Validar que existan las columnas necesarias.
* Excluir registros que no cumplan las reglas de calidad definidas.
* Calcular la duración de cada viaje.
* Guardar los viajes válidos y resúmenes por zona de recogida y por hora.
* Crear un reporte de calidad con cantidades de entrada, registros válidos y registros excluidos.

## Tecnologías

* Python 3
* Apache Spark 3.5.x
* PySpark 3.5.x
* Java compatible con la distribución de Spark instalada
* Archivo de entrada Parquet publicado por NYC TLC

## Estructura del repositorio

```text
procesamiento-batch-taxis/
├── src/
│   └── procesamiento\_batch.py
├── data/
│   └── README.md
├── resultados/              # Se genera al ejecutar; no se suben resultados grandes
├── requirements.txt
├── .gitignore
└── README.md
```

## Fuente de datos

Los datos oficiales se encuentran en la página de NYC TLC:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Para reproducir el análisis de este ejercicio se utilizó el archivo `yellow\_tripdata\_2026-08.parquet`. Por su tamaño y para respetar las condiciones de distribución de datos, el archivo no se incluye en este repositorio: descárguelo desde la fuente oficial y guárdelo en una ruta local.

## Requisitos previos

1. Tener Java instalado y configurado.
2. Tener Apache Spark 3.5.x instalado, con el comando `spark-submit` disponible en la terminal.
3. Usar PySpark de una versión compatible con la instalación de Spark. En el entorno de desarrollo de esta práctica se utilizó Spark 3.5.8 y PySpark 3.5.8.
4. Disponer de un archivo de entrada Parquet con las columnas requeridas.

Compruebe las versiones con:

```bash
java -version
spark-submit --version
python3 -m pip show pyspark
```

## Instalación

Clone el repositorio y entre en su carpeta:

```bash
git clone https://github.com/robertrcortes/procesamiento-batch-taxis.git
cd procesamiento-batch-taxis
```

Instale la dependencia Python, si necesita ejecutar el programa desde el entorno local:

```bash
python3 -m pip install -r requirements.txt
```

No es necesario instalar Kafka para ejecutar el procesamiento batch.

## Descargar y ubicar los datos

Descargue `yellow\_tripdata\_2026-08.parquet` desde la página oficial de NYC TLC y ubíquelo, por ejemplo, en la carpeta `data/`. La carpeta y el archivo de datos no se versionan en Git.

## Ejecución

Desde la raíz del repositorio, ejecute:

```bash
spark-submit src/procesamiento\_batch.py \\
  --input data/yellow\_tripdata\_2026-08.parquet \\
  --output resultados
```

En una máquina virtual donde el archivo esté ubicado en la ruta usada durante la práctica, puede ejecutar:

```bash
spark-submit src/procesamiento\_batch.py \\
  --input /home/vboxuser/bigdata/datos/yellow\_tripdata\_2026-08.parquet \\
  --output /home/vboxuser/bigdata/resultados\_batch
```

La primera ruta indica el archivo de entrada y la segunda el directorio donde Spark escribirá los resultados.

## Reglas de limpieza

El programa conserva los viajes que cumplan todas estas condiciones:

* Las fechas y horas de recogida y llegada son válidas.
* La llegada ocurre después de la recogida.
* La duración es mayor que cero y no supera 1.440 minutos (24 horas).
* Las zonas de recogida y llegada no son nulas.
* La distancia del viaje es mayor que cero.
* `fare\_amount` y `total\_amount` son valores no nulos y mayores o iguales a cero.

Estas son reglas de calidad definidas para el ejercicio; deben describirse en el informe como criterios aplicados, no como reglas oficiales de NYC TLC.

## Salidas generadas

Dentro de la carpeta indicada por `--output` se crean:

* `viajes\_limpios/`: registros válidos en formato Parquet.
* `resumen\_por\_zona/`: CSV con cantidad de viajes, distancia promedio, valor promedio y duración promedio por zona de recogida.
* `resumen\_por\_hora/`: CSV con cantidad de viajes y promedios por hora.
* `reporte\_calidad\_y\_resumen.json`: total de registros de entrada, registros válidos, excluidos, porcentaje excluido y reglas aplicadas.

Spark genera carpetas que contienen los archivos `part-\*` y metadatos; por eso los resultados CSV y Parquet aparecen como directorios, no como un único archivo con nombre fijo.

## Solución de problemas

* **No existe el archivo de entrada:** verifique la ruta pasada en `--input` y que el archivo Parquet esté descargado.
* **Faltan columnas requeridas:** revise que el archivo corresponda al conjunto de datos de viajes de taxi amarillo de NYC TLC y que incluya los campos mencionados.
* **Error de compatibilidad de Spark/PySpark:** compruebe que las versiones sean compatibles y coincidan con la instalación local. Para el entorno de esta práctica se recomienda Spark 3.5.8 y PySpark 3.5.8.
* **Permisos de escritura:** use una carpeta de salida sobre la que tenga permisos.

## Referencias

New York City Taxi and Limousine Commission. (s. f.). *TLC trip record data*. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Apache Software Foundation. (s. f.). *Spark SQL, DataFrames and Datasets guide*. https://spark.apache.org/docs/latest/sql-programming-guide.html

Apache Software Foundation. (s. f.). *Submitting applications*. https://spark.apache.org/docs/latest/submitting-applications.html


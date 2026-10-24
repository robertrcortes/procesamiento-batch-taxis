#!/usr/bin/env python3
"""Procesamiento batch de viajes de taxi NYC TLC usando Apache Spark."""

import argparse
import json
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_trunc,
    round as spark_round,
    to_timestamp,
    unix_timestamp,
)

REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "total_amount",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Procesa por lotes registros Parquet de viajes de taxi."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Ruta al archivo Parquet de entrada, por ejemplo data/yellow_tripdata_2026-08.parquet.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Directorio donde se guardarán los resultados.",
    )
    parser.add_argument(
        "--app-name",
        default="ProcesamientoBatchTaxisNYC",
        help="Nombre de la aplicación Spark.",
    )
    return parser.parse_args()


def ensure_required_columns(df: DataFrame) -> None:
    missing = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(
            "El archivo de entrada no contiene las columnas requeridas: "
            + ", ".join(missing)
        )


def json_safe(value: Any) -> Any:
    """Convierte valores Spark/Python a tipos serializables en JSON."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat(sep=" ") if hasattr(value, "hour") else value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser()
    output_path = Path(args.output).expanduser()

    if not input_path.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {input_path}")

    output_path.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.appName(args.app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Leyendo archivo Parquet: {input_path}")
        source = spark.read.parquet(str(input_path))
        ensure_required_columns(source)
        source = source.cache()
        total_input = source.count()
        print(f"Registros leídos: {total_input:,}")

        # Selección, tipado y creación de variables derivadas.
        prepared = source.select(
            to_timestamp(col("tpep_pickup_datetime")).alias("pickup_datetime"),
            to_timestamp(col("tpep_dropoff_datetime")).alias("dropoff_datetime"),
            col("PULocationID").cast("int").alias("pickup_zone"),
            col("DOLocationID").cast("int").alias("dropoff_zone"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("total_amount").cast("double").alias("total_amount"),
        ).withColumn(
            "duration_minutes",
            (unix_timestamp(col("dropoff_datetime"))
             - unix_timestamp(col("pickup_datetime"))) / 60.0,
        )

        # Reglas explícitas de calidad: fechas válidas, duración positiva
        # no superior a 24 horas, distancia positiva y valores no negativos.
        clean = prepared.filter(
            col("pickup_datetime").isNotNull()
            & col("dropoff_datetime").isNotNull()
            & (col("dropoff_datetime") > col("pickup_datetime"))
            & (col("duration_minutes") > 0)
            & (col("duration_minutes") <= 1440)
            & col("pickup_zone").isNotNull()
            & col("dropoff_zone").isNotNull()
            & col("trip_distance").isNotNull()
            & (col("trip_distance") > 0)
            & col("fare_amount").isNotNull()
            & (col("fare_amount") >= 0)
            & col("total_amount").isNotNull()
            & (col("total_amount") >= 0)
        ).cache()

        total_clean = clean.count()
        removed = total_input - total_clean
        print(f"Registros válidos después de la limpieza: {total_clean:,}")
        print(f"Registros excluidos por las reglas de calidad: {removed:,}")

        cleaned_output = output_path / "viajes_limpios"
        clean.write.mode("overwrite").parquet(str(cleaned_output))

        # Indicadores por zona de recogida.
        by_zone = (
            clean.groupBy("pickup_zone")
            .agg(
                count("*").alias("cantidad_viajes"),
                spark_round(avg("trip_distance"), 2).alias("distancia_promedio"),
                spark_round(avg("total_amount"), 2).alias("valor_promedio"),
                spark_round(avg("duration_minutes"), 2).alias("duracion_promedio_minutos"),
            )
            .orderBy(col("cantidad_viajes").desc())
        )
        by_zone_output = output_path / "resumen_por_zona"
        by_zone.coalesce(1).write.mode("overwrite").option("header", True).csv(str(by_zone_output))

        # Indicadores por hora de recogida.
        by_hour = (
            clean.withColumn("hora", date_trunc("hour", col("pickup_datetime")))
            .groupBy("hora")
            .agg(
                count("*").alias("cantidad_viajes"),
                spark_round(avg("trip_distance"), 2).alias("distancia_promedio"),
                spark_round(avg("total_amount"), 2).alias("valor_promedio"),
            )
            .orderBy("hora")
        )
        by_hour_output = output_path / "resumen_por_hora"
        by_hour.coalesce(1).write.mode("overwrite").option("header", True).csv(str(by_hour_output))

        # Resumen general y reporte de calidad en JSON.
        overall = clean.agg(
            count("*").alias("cantidad_viajes_validos"),
            spark_round(avg("trip_distance"), 2).alias("distancia_promedio"),
            spark_round(avg("total_amount"), 2).alias("valor_promedio"),
            spark_round(avg("duration_minutes"), 2).alias("duracion_promedio_minutos"),
        ).first().asDict()

        report = {
            "archivo_entrada": str(input_path),
            "total_registros_entrada": total_input,
            "total_registros_validos": total_clean,
            "registros_excluidos": removed,
            "porcentaje_excluido": round((removed / total_input * 100), 2) if total_input else 0.0,
            "reglas_calidad": [
                "Fechas de recogida y llegada válidas.",
                "La llegada debe ocurrir después de la recogida.",
                "Duración del viaje mayor que 0 y menor o igual a 1440 minutos.",
                "Zonas de recogida y llegada no nulas.",
                "Distancia del viaje mayor que 0.",
                "fare_amount y total_amount no nulos y mayores o iguales a 0.",
            ],
            "indicadores_generales": {key: json_safe(value) for key, value in overall.items()},
            "salidas": {
                "viajes_limpios": str(cleaned_output),
                "resumen_por_zona": str(by_zone_output),
                "resumen_por_hora": str(by_hour_output),
            },
        }
        report_path = output_path / "reporte_calidad_y_resumen.json"
        with report_path.open("w", encoding="utf-8") as file:
            json.dump(report, file, ensure_ascii=False, indent=2)

        print("\nProcesamiento batch finalizado correctamente.")
        print(f"Resultados guardados en: {output_path.resolve()}")
        print(f"Reporte de calidad: {report_path}")
        print("\nResumen general:")
        clean.select(
            count("*").alias("viajes_validos"),
        ).show(truncate=False)
        by_zone.show(10, truncate=False)
        return 0
    finally:
        try:
            source.unpersist()  # type: ignore[name-defined]
        except Exception:
            pass
        try:
            clean.unpersist()  # type: ignore[name-defined]
        except Exception:
            pass
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

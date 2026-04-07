"""
enricher.py — Enriquecimiento de la fact table de measurements.

Une las mediciones con las tablas de dimensiones (Locations, Paises)
para agregar country_code, country_name, provider_name, etc.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from etl.config import OUTPUT_DIR, YEAR


class MeasurementsEnricher:
    """
    Lee los parquets mensuales de measurements y los enriquece
    con datos de Locations y Paises.

    Resultado: measurements_enriched.parquet
    """

    def __init__(self, spark: SparkSession, output_dir: str = OUTPUT_DIR, year: int = YEAR):
        self.spark = spark
        self.output_dir = output_dir
        self.year = year
        self.output_path = os.path.join(output_dir, "measurements_enriched.parquet")

    def run(self):
        """Lee, enriquece y guarda."""
        print("\n[Enricher] Uniendo measurements con dimensiones...")

        # Leer fact table (todos los meses de golpe)
        df_measurements = self.spark.read.parquet(
            os.path.join(self.output_dir, f"measurements_{self.year}_*.parquet")
        )

        # Leer dimensiones
        df_locations = self.spark.read.parquet(
            os.path.join(self.output_dir, "Locations.parquet")
        )
        df_paises = self.spark.read.parquet(
            os.path.join(self.output_dir, "Paises.parquet")
        )

        # Asegurar tipos correctos para el join
        df_locations = df_locations.withColumn(
            "location_id", col("location_id").cast("integer")
        )

        # Join: measurements → Locations → Paises
        df_enriched = (
            df_measurements
            .join(
                df_locations.select(
                    "location_id", "country_code", "provider_name", "is_monitor"
                ),
                on="location_id",
                how="left",
            )
            .join(
                df_paises.select(
                    col("Pais_Code").alias("country_code"),
                    col("Pais").alias("country_name"),
                ),
                on="country_code",
                how="left",
            )
        )

        count = df_enriched.count()
        df_enriched.write.mode("overwrite").parquet(self.output_path)

        print(f"[Enricher] Guardado en {self.output_path} ({count:,} filas)")
        print("[Enricher] Esquema:")
        df_enriched.printSchema()

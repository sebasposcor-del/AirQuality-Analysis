"""
pipeline.py — Orquestador del ETL completo.

Ejecuta en orden:
1. Extracción de países europeos
2. Extracción de locations (estaciones)
3. Extracción de sensores
4. Descarga y transformación de measurements desde S3
5. Enriquecimiento (join con dimensiones)
"""

import os
from datetime import datetime

from etl.config import OUTPUT_DIR
from etl.spark_manager import SparkManager
from etl.extractors import (
    CountriesExtractor,
    LocationsExtractor,
    MeasurementsExtractor,
    SensorsExtractor,
)
from etl.enricher import MeasurementsEnricher


class ETLPipeline:
    """
    Orquesta la ejecución completa del ETL.

    Uso:
        pipeline = ETLPipeline()
        pipeline.run()
    """

    def __init__(self, output_dir: str = OUTPUT_DIR):
        self.output_dir = output_dir
        self.spark_manager = SparkManager()

    def run(self):
        """Ejecuta todo el pipeline ETL."""
        inicio = datetime.now()
        print("=" * 60)
        print("  AIR QUALITY EUROPE — ETL PIPELINE")
        print(f"  Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        os.makedirs(self.output_dir, exist_ok=True)

        # --------------------------------------------------
        # Paso 1: Países
        # --------------------------------------------------
        print("\n" + "─" * 40)
        print("PASO 1/5 — Extracción de países")
        print("─" * 40)
        countries_extractor = CountriesExtractor(self.output_dir)
        df_paises = countries_extractor.run()

        # --------------------------------------------------
        # Paso 2: Locations
        # --------------------------------------------------
        print("\n" + "─" * 40)
        print("PASO 2/5 — Extracción de locations")
        print("─" * 40)
        locations_extractor = LocationsExtractor(
            countries=countries_extractor.data,
            output_dir=self.output_dir,
        )
        df_locations = locations_extractor.run()

        # --------------------------------------------------
        # Paso 3: Sensores
        # --------------------------------------------------
        print("\n" + "─" * 40)
        print("PASO 3/5 — Extracción de sensores")
        print("─" * 40)
        sensors_extractor = SensorsExtractor(
            raw_locations=locations_extractor.raw_data,
            output_dir=self.output_dir,
        )
        df_sensores = sensors_extractor.run()

        # --------------------------------------------------
        # Paso 4: Measurements (S3 → Spark → Parquet)
        # --------------------------------------------------
        print("\n" + "─" * 40)
        print("PASO 4/5 — Extracción de measurements")
        print("─" * 40)
        spark = self.spark_manager.get_session()

        # Solo locations con isMonitor=True
        monitor_ids = df_locations[
            df_locations["is_monitor"] == True
        ]["location_id"].tolist()

        measurements_extractor = MeasurementsExtractor(
            spark=spark,
            location_ids=monitor_ids,
            output_dir=self.output_dir,
        )
        measurements_extractor.run()

        # --------------------------------------------------
        # Paso 5: Enriquecimiento
        # --------------------------------------------------
        print("\n" + "─" * 40)
        print("PASO 5/5 — Enriquecimiento (joins)")
        print("─" * 40)
        enricher = MeasurementsEnricher(spark=spark, output_dir=self.output_dir)
        enricher.run()

        # --------------------------------------------------
        # Resumen final
        # --------------------------------------------------
        self.spark_manager.stop()

        duracion = datetime.now() - inicio
        minutos = int(duracion.total_seconds() // 60)
        segundos = int(duracion.total_seconds() % 60)

        print("\n" + "=" * 60)
        print(f"  ETL COMPLETO — Duración total: {minutos}m {segundos}s")
        print(f"  Archivos en: {self.output_dir}")
        print("=" * 60)

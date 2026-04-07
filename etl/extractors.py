"""
extractors.py — Clases de extracción para cada capa del ETL.

Cada clase se encarga de obtener datos de una fuente (API o S3),
transformarlos a un formato tabular limpio y guardarlos como parquet.

Clases:
    CountriesExtractor  — países europeos desde OpenAQ API
    LocationsExtractor  — estaciones de monitoreo por país
    SensorsExtractor    — sensores por estación (filtrados por contaminante)
    MeasurementsExtractor — mediciones mensuales desde S3 vía PySpark
"""

import os
import time
from datetime import datetime

import boto3
import pandas as pd
import requests
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.functions import (
    col,
    dayofweek,
    hour,
    month,
    to_timestamp,
)

from etl.config import (
    EU_COUNTRY_CODES,
    OPENAQ_BASE_URL,
    OPENAQ_HEADERS,
    OUTLIER_THRESHOLDS,
    OUTPUT_DIR,
    PARAMS_INTERES,
    S3_BUCKET,
    YEAR,
)


# ============================================================
# 1. Países
# ============================================================
class CountriesExtractor:
    """
    Descarga todos los países de OpenAQ y filtra los europeos
    según EU_COUNTRY_CODES.

    Resultado: Paises.parquet con columnas [ID_Paises, Pais_Code, Pais]
    """

    def __init__(self, output_dir: str = OUTPUT_DIR):
        self.output_dir = output_dir
        self.output_path = os.path.join(output_dir, "Paises.parquet")
        self.data: list[dict] = []

    def extract(self) -> "CountriesExtractor":
        """Llama a la API y filtra países europeos."""
        response = requests.get(
            f"{OPENAQ_BASE_URL}/countries",
            headers=OPENAQ_HEADERS,
            params={"limit": 500},
        )
        response.raise_for_status()
        all_countries = response.json()["results"]

        self.data = [
            {
                "ID_Paises": row["id"],
                "Pais_Code": row["code"],
                "Pais": row["name"],
            }
            for row in all_countries
            if row["code"] in EU_COUNTRY_CODES
        ]

        print(f"[Países] {len(self.data)} países europeos encontrados")
        return self

    def save(self) -> pd.DataFrame:
        """Guarda como parquet y devuelve el DataFrame."""
        df = pd.DataFrame(self.data)
        df.to_parquet(self.output_path, index=False)
        print(f"[Países] Guardado en {self.output_path}")
        return df

    def run(self) -> pd.DataFrame:
        """Extrae y guarda en un solo paso."""
        return self.extract().save()


# ============================================================
# 2. Locations (estaciones de monitoreo)
# ============================================================
class LocationsExtractor:
    """
    Descarga todas las estaciones europeas, paginando por país.
    Aplana el JSON anidado en un esquema plano.

    Resultado: Locations.parquet
    """

    def __init__(self, countries: list[dict], output_dir: str = OUTPUT_DIR):
        """
        Args:
            countries: lista de dicts con al menos 'ID_Paises' y 'Pais_Code'.
        """
        self.countries = countries
        self.output_dir = output_dir
        self.output_path = os.path.join(output_dir, "Locations.parquet")
        self.raw_data: list[dict] = []  # JSON crudo de la API
        self.data: list[dict] = []      # Datos aplanados

    def extract(self) -> "LocationsExtractor":
        """Pagina por cada país y acumula todas las locations."""
        for pais in self.countries:
            page = 1
            while True:
                response = requests.get(
                    f"{OPENAQ_BASE_URL}/locations",
                    headers=OPENAQ_HEADERS,
                    params={
                        "limit": 1000,
                        "countries_id": pais["ID_Paises"],
                        "page": page,
                    },
                )
                data = response.json()

                if "results" not in data or not data["results"]:
                    break

                self.raw_data.extend(data["results"])

                # Si devolvió menos de 1000, ya no hay más páginas
                if len(data["results"]) < 1000:
                    break

                page += 1
                time.sleep(0.3)

            print(f"  {pais['Pais_Code']}: descargado — acumulado: {len(self.raw_data)}")

        print(f"[Locations] Total descargadas: {len(self.raw_data)}")
        return self

    def _flatten(self) -> list[dict]:
        """Aplana el JSON anidado a un esquema tabular."""
        flat = []
        for loc in self.raw_data:
            flat.append({
                "location_id":    loc["id"],
                "location_name":  loc["name"],
                "country_id":     loc["country"]["id"],
                "country_code":   loc["country"]["code"],
                "provider_id":    loc["provider"]["id"],
                "provider_name":  loc["provider"]["name"],
                "is_monitor":     loc["isMonitor"],
                "lat":            loc["coordinates"]["latitude"],
                "lon":            loc["coordinates"]["longitude"],
                "datetime_first": (loc.get("datetimeFirst") or {}).get("utc"),
                "datetime_last":  (loc.get("datetimeLast") or {}).get("utc"),
            })
        return flat

    def save(self) -> pd.DataFrame:
        """Aplana, guarda y devuelve el DataFrame."""
        self.data = self._flatten()
        df = pd.DataFrame(self.data)
        df.to_parquet(self.output_path, index=False)
        print(f"[Locations] Guardado en {self.output_path} ({len(df)} filas)")
        return df

    def run(self) -> pd.DataFrame:
        return self.extract().save()


# ============================================================
# 3. Sensores
# ============================================================
class SensorsExtractor:
    """
    Extrae sensores desde los datos crudos de locations,
    filtrando por PARAMS_INTERES.

    Resultado: Sensores.parquet
    """

    def __init__(self, raw_locations: list[dict], output_dir: str = OUTPUT_DIR):
        """
        Args:
            raw_locations: lista del JSON crudo de locations (con campo 'sensors').
        """
        self.raw_locations = raw_locations
        self.output_dir = output_dir
        self.output_path = os.path.join(output_dir, "Sensores.parquet")

    def run(self) -> pd.DataFrame:
        """Extrae, filtra y guarda."""
        sensors = []
        for loc in self.raw_locations:
            for sens in loc.get("sensors", []):
                param = sens["parameter"]["name"]
                if param in PARAMS_INTERES:
                    sensors.append({
                        "sensor_id":   sens["id"],
                        "location_id": loc["id"],
                        "param":       param,
                        "units":       sens["parameter"]["units"],
                    })

        df = pd.DataFrame(sensors)
        df.to_parquet(self.output_path, index=False)
        print(f"[Sensores] Guardados: {len(df)} sensores en {self.output_path}")
        return df


# ============================================================
# 4. Measurements (fact table)
# ============================================================
class MeasurementsExtractor:
    """
    Descarga mediciones del año indicado desde el bucket S3 público de OpenAQ.

    Proceso:
    1. boto3 verifica qué locations tienen datos en S3 (acceso anónimo)
    2. Agrupa los paths válidos por mes
    3. Spark lee los CSV.GZ en paralelo, aplica transformaciones y guarda parquet

    Resultado: measurements_{YEAR}_MM.parquet (uno por mes)
    """

    # Esquema CSV del bucket S3
    SCHEMA = StructType([
        StructField("location_id", IntegerType()),
        StructField("sensors_id",  IntegerType()),
        StructField("location",    StringType()),
        StructField("datetime",    StringType()),
        StructField("lat",         DoubleType()),
        StructField("lon",         DoubleType()),
        StructField("parameter",   StringType()),
        StructField("units",       StringType()),
        StructField("value",       DoubleType()),
    ])

    def __init__(
        self,
        spark: SparkSession,
        location_ids: list[int],
        year: int = YEAR,
        output_dir: str = OUTPUT_DIR,
    ):
        self.spark = spark
        self.location_ids = location_ids
        self.year = year
        self.output_dir = output_dir
        self.paths_por_mes: dict[str, list[str]] = {
            f"{m:02d}": [] for m in range(1, 13)
        }

    # --------------------------------------------------------
    # Paso 1: Verificar paths en S3 con boto3
    # --------------------------------------------------------
    def verify_s3_paths(self) -> "MeasurementsExtractor":
        """
        Para cada location_id, consulta S3 una sola vez (todo el año)
        y agrupa los paths válidos por mes.
        """
        s3 = boto3.client("s3", config=BotoConfig(signature_version=UNSIGNED))
        total = len(self.location_ids)

        print(f"[Measurements] Verificando {total} locations en S3...")

        for i, loc_id in enumerate(self.location_ids):
            prefix = f"records/csv.gz/locationid={loc_id}/year={self.year}/"
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

            if response.get("KeyCount", 0) == 0:
                continue

            # Extraer meses disponibles del listado de archivos
            meses_encontrados = set()
            for obj in response.get("Contents", []):
                key = obj["Key"]
                # El path tiene la forma: .../month=01/...
                mes = key.split("month=")[1].split("/")[0]
                meses_encontrados.add(mes)

            for mes in meses_encontrados:
                s3a_path = (
                    f"s3a://{S3_BUCKET}/records/csv.gz/"
                    f"locationid={loc_id}/year={self.year}/month={mes}/"
                )
                self.paths_por_mes[mes].append(s3a_path)

            if (i + 1) % 500 == 0:
                print(f"  Verificadas {i + 1}/{total} locations...")

        print("[Measurements] Verificación S3 completa:")
        for mes, paths in self.paths_por_mes.items():
            print(f"  Mes {mes}: {len(paths)} paths válidos")

        return self

    # --------------------------------------------------------
    # Paso 2: Leer desde S3 con Spark, transformar y guardar
    # --------------------------------------------------------
    def process_month(self, mes: str) -> int:
        """
        Lee los CSV.GZ de un mes desde S3, aplica transformaciones
        y guarda como parquet. Devuelve el número de registros.
        """
        output_path = os.path.join(
            self.output_dir, f"measurements_{self.year}_{mes}.parquet"
        )

        # Saltar si ya existe
        if os.path.exists(output_path):
            print(f"  Mes {mes} ya procesado, saltando...")
            return 0

        paths_validos = self.paths_por_mes[mes]
        if not paths_validos:
            print(f"  Mes {mes}: sin datos, saltando...")
            return 0

        inicio = datetime.now()
        print(f"\n  Procesando mes {mes} — inicio: {inicio.strftime('%H:%M:%S')}")

        # Lectura desde S3
        df = (
            self.spark.read
            .option("header", "true")
            .schema(self.SCHEMA)
            .csv(paths_validos)
        )

        # Transformaciones
        df = self._transform(df)

        count = df.count()
        print(f"  Registros válidos: {count:,}")

        df.write.mode("overwrite").parquet(output_path)

        duracion = datetime.now() - inicio
        minutos = int(duracion.total_seconds() // 60)
        segundos = int(duracion.total_seconds() % 60)
        print(f"  Mes {mes} completo — {minutos}m {segundos}s — guardado en {output_path}")

        return count

    def process_all_months(self) -> None:
        """Procesa los 12 meses secuencialmente."""
        print(f"\n[Measurements] Procesando {self.year}...")
        total = 0
        for mes in [f"{m:02d}" for m in range(1, 13)]:
            total += self.process_month(mes)
        print(f"\n[Measurements] Total registros {self.year}: {total:,}")

    def run(self) -> None:
        """Verifica S3 y procesa todos los meses."""
        self.verify_s3_paths()
        self.process_all_months()

    # --------------------------------------------------------
    # Transformaciones
    # --------------------------------------------------------
    @staticmethod
    def _transform(df):
        """
        Aplica limpieza y feature engineering al DataFrame de un mes:
        - Filtra valores negativos
        - Filtra outliers por contaminante
        - Parsea datetime
        - Agrega columnas temporales (hora, dia_semana, mes, es_fin_semana)
        """
        # 1. Eliminar valores negativos
        df = df.filter(col("value") >= 0.0)

        # 2. Eliminar outliers por contaminante
        outlier_condition = None
        for param, threshold in OUTLIER_THRESHOLDS.items():
            cond = (col("parameter") == param) & (col("value") > threshold)
            outlier_condition = cond if outlier_condition is None else (outlier_condition | cond)

        if outlier_condition is not None:
            df = df.filter(~outlier_condition)

        # 3. Parsear datetime
        df = df.withColumn("datetime", to_timestamp(col("datetime")))

        # 4. Columnas temporales
        df = (
            df
            .withColumn("hora",          hour(col("datetime")))
            .withColumn("dia_semana",    dayofweek(col("datetime")))
            .withColumn("mes",           month(col("datetime")))
            .withColumn("es_fin_semana", dayofweek(col("datetime")).isin([1, 7]))
        )

        return df

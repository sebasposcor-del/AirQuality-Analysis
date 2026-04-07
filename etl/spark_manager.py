"""
spark_manager.py para gestión centralizada de la SparkSession.

Crea, configura y cierra la sesión de Spark con los parámetros
necesarios para leer S3 con acceso anónimo.
"""

from pyspark.sql import SparkSession
from etl.config import (
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_SHUFFLE_PARTITIONS,
)


class SparkManager:
    """
    Encapsula la creación y destrucción de la SparkSession.

    Uso típico:
        manager = SparkManager()
        spark = manager.get_session()
        # ... trabajar con spark ...
        manager.stop()
    """

#_spark es para privacidad, para usar solo a través de get_session() y stop()
    def __init__(self):
        self._spark = None

    def get_session(self) -> SparkSession:
        """Devuelve la SparkSession, creándola si no existe."""
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName(SPARK_APP_NAME)
                .master("local[*]")
                .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
                .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
                )
                .getOrCreate()
            )
            # Reducir verbosidad de logs de Spark
            self._spark.sparkContext.setLogLevel("WARN")
            print(f"SparkSession creada — versión: {self._spark.version}")
        return self._spark

    def stop(self):
        """Cierra la SparkSession si está activa."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            print("SparkSession cerrada.")

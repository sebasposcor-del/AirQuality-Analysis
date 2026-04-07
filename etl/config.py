"""
config.py — Constantes y configuración del ETL de Air Quality Europe.

Centraliza API keys, códigos ISO europeos, parámetros de interés,
umbrales de outliers y rutas de salida.
"""

# ============================================================
# API OpenAQ v3
# ============================================================
OPENAQ_API_KEY = "6536ed59b3867122e71ff031e911243815f3e1002f82f6fd03cc63f0fae54f08"
OPENAQ_BASE_URL = "https://api.openaq.org/v3"
OPENAQ_HEADERS = {"X-API-Key": OPENAQ_API_KEY}

# ============================================================
# S3 — bucket público de OpenAQ
# ============================================================
S3_BUCKET = "openaq-data-archive"
S3_PREFIX_TEMPLATE = "records/csv.gz/locationid={loc_id}/year={year}/"

# ============================================================
# Códigos ISO de países europeos
# ============================================================
EU_COUNTRY_CODES = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
    "NO", "CH", "GB", "IS", "RS", "BA", "ME", "MK", "AL",
]

# ============================================================
# Contaminantes de interés y umbrales máximos (outliers)
# ============================================================
PARAMS_INTERES = {"pm25", "pm10", "no2", "o3", "so2", "co"}

# Valor máximo aceptable por contaminante (valores superiores = outlier)
OUTLIER_THRESHOLDS = {
    "pm25": 1000.0,
    "pm10": 1000.0,
    "no2":  1000.0,
    "o3":   1000.0,
    "so2":  1000.0,
    "co":   10000.0,
}

# ============================================================
# Año de extracción
# ============================================================
YEAR = 2023

# ============================================================
# Rutas de salida (dentro del contenedor Docker)
# El volumen ./data:/app/data hace que persistan en tu máquina
# ============================================================
OUTPUT_DIR = "/app/data"

# ============================================================
# Spark
# ============================================================
SPARK_APP_NAME = "OpenAQ_ETL"
SPARK_DRIVER_MEMORY = "8g"
SPARK_SHUFFLE_PARTITIONS = 16

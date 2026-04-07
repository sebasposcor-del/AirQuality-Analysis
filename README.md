# Air Quality Europe — ETL Pipeline

**Proyecto TFM MD008/MD009 — La Salle, Ramon Llull**

Pipeline de extracción y transformación de datos de calidad del aire en Europa usando OpenAQ API v3 y AWS S3, procesado con PySpark.

## Estructura del proyecto

```
airquality-analysis/
├── main.py                  # Punto de entrada
├── etl/
│   ├── __init__.py
│   ├── config.py            # Constantes (API key, códigos EU, paths)
│   ├── spark_manager.py     # Gestión de SparkSession
│   ├── extractors.py        # Extractores: Países, Locations, Sensores, Measurements
│   ├── enricher.py          # Join de measurements con dimensiones
│   └── pipeline.py          # Orquestador del ETL completo
├── data/                    # Parquets generados (gitignored)
├── Dockerfile
├── docker-compose.yml
└── .gitignore
```

## Tablas generadas

| Archivo | Tipo | Descripción |
|---------|------|-------------|
| `Paises.parquet` | Dimensión | Países europeos |
| `Locations.parquet` | Dimensión | Estaciones de monitoreo |
| `Sensores.parquet` | Dimensión | Sensores filtrados (pm25, pm10, no2, o3, so2, co) |
| `measurements_2023_MM.parquet` | Fact | Mediciones por mes |
| `measurements_enriched.parquet` | Fact | Mediciones con country_code, country_name, provider |

## Cómo ejecutar

```bash
# 1. Construir la imagen (solo la primera vez o si cambias el Dockerfile)
docker compose build
s
# 2. Ejecutar el ETL completo
docker compose run --rm etl

# 3. Los parquets quedan en ./data/
```

## Requisitos

- Docker y Docker Compose
- ~8 GB de RAM libre (para Spark)
- Conexión a internet (API OpenAQ + S3)

## Dependencias (dentro del contenedor)

- Python 3.11
- PySpark 3.5.1
- boto3 1.34.0
- pandas 2.1.4
- pyarrow 14.0.2
- Java 21 (OpenJDK)

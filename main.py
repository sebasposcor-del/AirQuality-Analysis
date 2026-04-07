"""
main.py — Punto de entrada del ETL de Air Quality Europe.

Ejecuta el pipeline completo con un solo comando:
    python main.py
"""

from etl.pipeline import ETLPipeline


if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()

"""
Processing-Microservice (Apache Spark)
========================================
Batch-Verarbeitung der im HDFS Raw-Layer abgelegten Flugdaten:

  1. Bereinigung: Duplikate entfernen, Datensätze mit fehlenden
     Kernattributen (Datum, Start-/Zielflughafen) verwerfen
  2. Feature-Generierung: Wochenend-Indikator, Saison (Jahreszeit)
     aus dem Flugdatum ableiten
  3. Aggregation: durchschnittliche Abflug-/Ankunftsverspätung und
     Fluganzahl je Flugdatum und Flughafen-Paar

Die aggregierten Ergebnisse werden sowohl als Parquet in den HDFS
Aggregation-Layer geschrieben (für Nachvollziehbarkeit / Re-Processing)
als auch per JDBC in die PostgreSQL Serving-Layer-Tabelle upserted, aus
der die nachgelagerte ML-Applikation liest.
"""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

HDFS_NAMENODE_URL = os.environ.get("HDFS_NAMENODE_URL", "hdfs://namenode:9000")
HDFS_RAW_PATH = os.environ.get("HDFS_RAW_PATH", "/raw/flights")
HDFS_AGGREGATION_PATH = os.environ.get("HDFS_AGGREGATION_PATH", "/aggregation/flights")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "flight_data")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "batch_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "batch_pw")

COL_DATE = "FL_DATE"
COL_ORIGIN = "ORIGIN"
COL_DEST = "DEST"
COL_DEP_DELAY = "DEP_DELAY"
COL_ARR_DELAY = "ARR_DELAY"


def season_from_month(month_col):
    return (
        F.when(month_col.isin(12, 1, 2), "winter")
        .when(month_col.isin(3, 4, 5), "spring")
        .when(month_col.isin(6, 7, 8), "summer")
        .otherwise("autumn")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("flight-delay-batch-processing")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE_URL)
        .getOrCreate()
    )

    raw_path = f"{HDFS_NAMENODE_URL}{HDFS_RAW_PATH}"
    print(f"Lese Raw-Layer aus {raw_path} ...")
    df = spark.read.parquet(raw_path)

    df = df.dropDuplicates()
    df = df.dropna(subset=[COL_DATE, COL_ORIGIN, COL_DEST])
    df = df.withColumn(COL_DEP_DELAY, F.col(COL_DEP_DELAY).cast(DoubleType()))
    df = df.withColumn(COL_ARR_DELAY, F.col(COL_ARR_DELAY).cast(DoubleType()))
    df = df.withColumn("flight_date", F.to_date(F.col(COL_DATE)))

    df = df.withColumn("day_of_week", F.dayofweek("flight_date"))
    df = df.withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
    df = df.withColumn("month", F.month("flight_date"))
    df = df.withColumn("season", season_from_month(F.col("month")))

    aggregated = (
        df.groupBy("flight_date", F.col(COL_ORIGIN).alias("origin_airport"),
                    F.col(COL_DEST).alias("dest_airport"), "is_weekend", "season")
        .agg(
            F.avg(COL_DEP_DELAY).alias("avg_departure_delay"),
            F.avg(COL_ARR_DELAY).alias("avg_arrival_delay"),
            F.count("*").alias("flight_count"),
        )
    )

    aggregation_output_path = f"{HDFS_NAMENODE_URL}{HDFS_AGGREGATION_PATH}"
    print(f"Schreibe Aggregation-Layer nach {aggregation_output_path} ...")
    aggregated.write.mode("overwrite").parquet(aggregation_output_path)

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    print(f"Schreibe Serving-Layer nach {jdbc_url} ...")
    (
        aggregated.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "flight_delay_aggregates_staging")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    upsert_sql = """
        INSERT INTO flight_delay_aggregates
            (flight_date, origin_airport, dest_airport, avg_departure_delay,
             avg_arrival_delay, flight_count, is_weekend, season)
        SELECT flight_date, origin_airport, dest_airport, avg_departure_delay,
               avg_arrival_delay, flight_count, is_weekend, season
        FROM flight_delay_aggregates_staging
        ON CONFLICT (flight_date, origin_airport, dest_airport)
        DO UPDATE SET
            avg_departure_delay = EXCLUDED.avg_departure_delay,
            avg_arrival_delay = EXCLUDED.avg_arrival_delay,
            flight_count = EXCLUDED.flight_count,
            is_weekend = EXCLUDED.is_weekend,
            season = EXCLUDED.season,
            processed_at = now();
    """
    import psycopg2
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
            cur.execute("DROP TABLE IF EXISTS flight_delay_aggregates_staging;")
        conn.commit()
    finally:
        conn.close()

    print("Batch-Processing abgeschlossen.")
    spark.stop()


if __name__ == "__main__":
    main()

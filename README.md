# Batch-basierte Datenarchitektur für eine datenintensive Applikation

Portfolio-Projekt DLMDWWDE02 – Aufgabe 1: Entwickle eine batch-basierte Datenarchitektur für eine datenintensive Applikation.

Dieses Projekt implementiert eine Microservice-basierte Batch-Dateninfrastruktur für die Aufbereitung historischer Flugdaten als Backend für eine (nicht Teil dieses Projekts befindliche) Machine-Learning-Applikation zur Prognose von Flugverspätungen. Die Pipeline lädt Flugdaten quartalsweise über Kafka, speichert sie im HDFS Raw-Layer, bereinigt und aggregiert sie mit Apache Spark und stellt die finalen Kennzahlen in einer PostgreSQL-Datenbank als Serving Layer bereit.

Eine ausführliche Beschreibung der Architektur inkl. Diagramm findest Du in docs/architecture.md.

## Projektstruktur

batch-data-architecture/
├── docker-compose.yml
├── .env.example
├── ingestion/
│   ├── data/
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile.producer
├── storage/
│   ├── hdfs_loader.py
│   ├── requirements.txt
│   ├── Dockerfile.loader
│   └── init.sql
├── processing/
│   ├── spark_job.py
│   └── Dockerfile
└── docs/
    └── architecture.md

## Voraussetzungen

- Docker & Docker Compose (v2)
- Ein Flugdaten-Datensatz (Kaggle: "Airline Delay and Cancellation Data", mindestens 1.000.000 Datenpunkte mit Zeitstempel), abgelegt unter ingestion/data/flights.csv

## Setup & Ausführung

1. Repository klonen und in den Ordner wechseln
2. Umgebungsvariablen konfigurieren: cp .env.example .env
3. Flugdaten-Datensatz unter ingestion/data/flights.csv ablegen
4. Infrastruktur starten: docker compose up -d zookeeper kafka namenode datanode postgres adminer
5. Ingestion und Storage-Loader starten: docker compose up --build ingestion-producer hdfs-loader
6. Sobald die Rohdaten im HDFS Raw-Layer liegen, den Batch-Job starten: docker compose up --build spark-processor

Danach:
- HDFS-Web-UI: http://localhost:9870
- Adminer (DB inspizieren, System: PostgreSQL, Server: postgres): http://localhost:8081

## Datenfluss im Überblick

1. ingestion-producer liest den Flugdaten-Datensatz und publiziert jede Zeile inkl. ingestion_timestamp als JSON nach Kafka-Topic flight-data-raw (simuliert die quartalsweise Datenlieferung).
2. hdfs-loader konsumiert den Kafka-Strom, sammelt Batches und schreibt sie als Parquet-Dateien in den HDFS Raw-Layer.
3. spark-processor liest den Raw-Layer, bereinigt die Daten, generiert Features (Wochenend-Indikator, Saison) und aggregiert je Flugdatum und Flughafen-Paar.
4. Die Aggregate werden sowohl in den HDFS Aggregation-Layer als auch per Upsert in die PostgreSQL-Tabelle flight_delay_aggregates geschrieben.
5. Eine ML-Applikation (außerhalb dieses Projekts) könnte diese Tabelle als Trainingsgrundlage abfragen.

## Reflexion / Making-of

Siehe docs/architecture.md für die Diskussion von Reliability/Scalability/Maintainability, Data Governance und den Trade-offs der gewählten Architektur.

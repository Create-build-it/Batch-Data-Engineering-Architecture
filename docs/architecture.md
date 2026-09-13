# Architektur – Batch-basierte Datenarchitektur für eine datenintensive Applikation

## Übersicht

Der Datenfluss verläuft in vier Schichten:

1. Ingestion Layer (ingestion-producer) – liest historische Flugdaten aus einer CSV-Quelle und publiziert jede Zeile inkl. Zeitstempel nach Kafka, simuliert damit die quartalsweise Datenlieferung
2. Messaging (Kafka) – Topic flight-data-raw puffert die Rohdaten und entkoppelt Ingestion von Storage
3. Storage Layer (hdfs-loader, HDFS) – konsumiert den Kafka-Strom und schreibt Batches als Parquet-Dateien in den Raw-Layer; nach der Verarbeitung liegt zusätzlich ein Aggregation-Layer vor
4. Processing Layer (spark-processor, Apache Spark) – bereinigt die Rohdaten, generiert Features (Wochenend-Indikator, Saison) und aggregiert je Flugdatum und Flughafen-Paar
5. Serving Layer (PostgreSQL) – persistiert die finalen Kennzahlen in der Tabelle flight_delay_aggregates als Grundlage für eine (nicht Teil dieses Projekts befindliche) ML-Applikation zur Flugverspätungs-Prognose

## Microservices und ihre Aufgaben

- ingestion-producer: Data Ingestion, simuliert die quartalsweise Aufnahme historischer Flugdaten aus einer CSV-Quelle und publiziert sie inkl. Zeitstempel nach Kafka (Python, confluent-kafka, pandas)
- zookeeper / kafka: Messaging-Backbone, puffert die Rohdaten und entkoppelt Ingestion von Storage
- hdfs-loader: konsumiert den Kafka-Strom und schreibt Batches als Parquet-Dateien in den HDFS Raw-Layer (Python, WebHDFS)
- namenode / datanode: verteiltes Speichersystem für Raw- und Aggregation-Layer (Hadoop HDFS)
- spark-processor: Bereinigung, Feature-Generierung und Aggregation der Flugdaten (Apache Spark, PySpark)
- postgres: Serving Layer, finale aggregierte Kennzahlen für die ML-Applikation

## Reliability, Scalability, Maintainability

- Reliability: Kafka puffert und repliziert Nachrichten zwischen Ingestion und Storage; der Producer nutzt acks=all und Retries. Der Spark-Job führt einen Upsert (ON CONFLICT ... DO UPDATE) statt reiner Inserts aus, sodass wiederholte oder fehlgeschlagene Batch-Läufe keine Duplikate erzeugen.
- Scalability: HDFS ist horizontal durch weitere Datanodes erweiterbar, Spark kann durch zusätzliche Worker-Knoten skaliert werden. Jede Komponente läuft als eigener, unabhängiger Container.
- Maintainability: Klare Trennung in Ingestion/Storage/Processing/Serving, Konfiguration ausschließlich über Umgebungsvariablen, Infrastructure as Code via docker-compose.yml, sodass das g

# Architektur – Real-time Backend für datenintensive Applikation

## Übersicht

Der Datenfluss verläuft in vier Schichten:

1. Ingestion Layer (sensor-producer) – simuliert IoT-Sensoren, publiziert JSON-Messwerte inkl. Zeitstempel nach Kafka
2. Messaging Layer (Zookeeper + Kafka) – Topic sensor-readings puffert und entkoppelt Ingestion von Processing
3. Processing Layer (stream-processor, Faust) – konsumiert den Stream, aggregiert in Tumbling Windows (avg/min/max/count), schreibt Ergebnisse zurück nach Kafka (Topic sensor-aggregates) und in die Datenbank
4. Storage Layer (TimescaleDB) – persistiert die Aggregate in der Hypertable sensor_aggregates als Grundlage für eine (nicht Teil dieses Projekts befindliche) Echtzeit-Reporting-Applikation

## Microservices und ihre Aufgaben

- sensor-producer: Data Ingestion, simuliert eine Flotte von IoT-Sensoren und publiziert Messwerte inkl. Zeitstempel nach Kafka (Python, confluent-kafka)
- zookeeper / kafka: Messaging-Backbone, entkoppelt Ingestion und Processing, puffert Nachrichten
- stream-processor: konsumiert den Rohdaten-Stream, aggregiert in Tumbling Windows pro Sensor, schreibt Ergebnis-Topic und persistiert in die DB (Faust, Kafka Streams für Python)
- timescaledb: persistente Ablage der aggregierten Zeitreihendaten als Hypertable

## Reliability, Scalability, Maintainability

- Reliability: Kafka repliziert und puffert Nachrichten; der Producer nutzt acks=all und Retries. Verarbeitungsfehler beim DB-Schreiben werden abgefangen und geloggt, ohne den Stream-Prozess abzubrechen.
- Scalability: Jeder Service läuft als eigener, unabhängiger Container (Microservice-Architektur). Kafka-Topics sind partitioniert, sodass der stream-processor horizontal skaliert werden kann. TimescaleDB partitioniert Daten automatisch per Hypertable-Chunking.
- Maintainability: Klare Trennung in Ingestion/Processing/Storage, Konfiguration ausschließlich über Umgebungsvariablen, Infrastructure as Code via docker-compose.yml, sodass das gesamte System mit docker compose up reproduzierbar ist.

## Datenschutz, Datensicherheit, Data Governance

- Zugangsdaten werden nicht im Code, sondern über .env (nicht versioniert) verwaltet
- Container laufen als Non-Root-User
- Für die Reporting-Applikation existiert ein separater, nur lesender DB-User (reporting_readonly) nach dem Prinzip der geringsten Rechte
- Eine Retention Policy löscht Aggregate automatisch nach 90 Tagen (Data-Minimierung)
- Die Rohdaten sind bereits im Producer synthetisch (keine personenbezogenen Daten)

## Windowing-Strategie

Es wird ein Tumbling Window (feste, nicht überlappende Fensterbreite) verwendet. Vorteil: einfach zu implementieren, jedes Event gehört zu genau einem Fenster. Nachteil: bei Ausreißern an Fenstergrenzen können Trends leicht verzögert sichtbar werden. Alternativen wie Hopping/Sliding Windows (überlappend, glättet Ausreißer, höherer Rechenaufwand) oder Session Windows (dynamische Fenster basierend auf Aktivitätspausen) wurden bewusst nicht gewählt, da bei kontinuierlichen Sensordaten eine feste, gut vorhersehbare Fensterbreite für ein Reporting-Backend ausreichend und ressourcenschonender ist.

## Vor- und Nachteile der Architektur

Vorteile: lose gekoppelte Microservices, unabhängig skalier- und austauschbar; Kafka als Puffer entkoppelt Ingestion-Rate von Processing-Rate; Faust ermöglicht Kafka-Streams-artiges Processing rein in Python, ohne JVM/Spark-Cluster-Overhead; TimescaleDB kombiniert SQL-Komfort mit Zeitreihen-Performance.

Nachteile/Trade-offs: Faust ist weniger ausgereift als Kafka Streams (Java) oder Flink für sehr große, produktionskritische Deployments; aktuell Single-Broker-Kafka (kein Replikationsfaktor größer 1); Aggregation im Prozessspeicher geht bei Container-Neustart zwischen zwei Flush-Intervallen verloren.

## Strategie zur Integration einer zweiten (Batch-)Pipeline

Für nachgelagerte Analysen, die nicht in Echtzeit erfolgen müssen, könnte zusätzlich ein Batch-Pfad ergänzt werden: Ein Kafka-Consumer schreibt die Rohdaten zusätzlich in einen Object Store im Parquet-Format. Ein zeitgesteuerter Batch-Job liest periodisch die Parquet-Dateien, führt komplexere Aggregationen durch und schreibt die Ergebnisse in eine separate Tabelle. Die bestehende TimescaleDB könnte dabei sowohl Echtzeit- als auch Batch-Aggregate aufnehmen, sodass das Reporting beide Datenquellen konsolidiert abfragen kann.

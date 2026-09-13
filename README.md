Real-time Backend für eine datenintensive Applikation

Portfolio-Projekt DLMDWWDE02 – Aufgabe 2: Entwickle ein real-time Backend für eine datenintensive Applikation.

Dieses Projekt implementiert eine Microservice-basierte Streaming-Dateninfrastruktur, die kontinuierlich simulierte IoT-Sensordaten aufnimmt, in Kafka puffert, mittels Tumbling-Window-Aggregation (Kafka Streams / Faust) verarbeitet und die Ergebnisse in TimescaleDB für eine (nicht Teil dieses Projekts befindliche) Echtzeit-Reporting-Applikation bereitstellt.

Eine ausführliche Beschreibung der Architektur inkl. Diagramm findest Du in docs/architecture.md.

Projektstruktur

realtime-data-architecture/
├── docker-compose.yml        # Infrastructure as Code – gesamter Stack
├── .env.example               # Vorlage für Umgebungsvariablen (Secrets)
├── ingestion/                  # Microservice: Sensor-Daten-Simulation
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
├── processing/                 # Microservice: Kafka Streams (Faust) Aggregation
│   ├── stream_processor.py
│   ├── requirements.txt
│   └── Dockerfile
├── storage/                    # TimescaleDB Schema (Infrastructure as Code)
│   └── init.sql
└── docs/
└── architecture.md         # Architektur-Diagramm & Reflexion

Voraussetzungen

	•	Docker & Docker Compose (v2)
	•	Keine weitere lokale Installation nötig – alle Services laufen containerisiert

Setup & Ausführung

	1.	Repository klonen: git clone <REPO_URL> und cd realtime-data-architecture
	2.	Umgebungsvariablen konfigurieren: cp .env.example .env
	3.	Gesamten Stack starten: docker compose up --build

Danach:

	•	Kafka-UI (Topics beobachten): http://localhost:8080
	•	Adminer (DB inspizieren, System: PostgreSQL, Server: timescaledb): http://localhost:8081

Datenfluss im Überblick
1.	sensor-producer erzeugt alle EMIT_INTERVAL_SECONDS Sekunden für NUM_SENSORS simulierte Sensoren neue Messwerte inkl. Zeitstempel und publiziert sie als JSON nach Kafka-Topic sensor-readings.
	2.	stream-processor konsumiert den Stream, puffert Werte pro Sensor und schließt alle WINDOW_SECONDS Sekunden ein Tumbling Window: es berechnet avg, min, max und count je Sensor.
	3.	Die Aggregate werden sowohl auf das Kafka-Topic sensor-aggregates publiziert als auch direkt in die Hypertable sensor_aggregates in TimescaleDB geschrieben.
	4.	Eine Reporting-Applikation (außerhalb dieses Projekts) könnte die Tabelle sensor_aggregates per SQL abfragen.

Reflexion / Making-of

Siehe docs/architecture.md für die Diskussion von Reliability/Scalability/Maintainability, Data Governance und den Trade-offs der gewählten Architektur.

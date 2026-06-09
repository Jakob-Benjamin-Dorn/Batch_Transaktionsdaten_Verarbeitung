# Batch-Transaktionsdaten-Verarbeitung 


### Startanleitung: 

Benötigt werden Docker, Docker Compose und Git. Das Repository kann geklont werden mit:

```bash
git clone https://github.com/Jakob-Benjamin-Dorn/Batch_Transaktionsdaten_Verarbeitung.git
cd Batch_Transaktionsdaten_Verarbeitung
```

Beim ersten Start muss das Docker-Image gebaut und die Services gestartet werden:

```bash
docker compose up --build -d
```

Bei späteren Starts:

```bash
docker compose up -d
```

Der Initialisierungs-Service richtet automatisch die benötigten Docker Volumes und deren Berechtigungen ein.

### Status prüfen

```bash
docker compose ps
```

Logs anzeigen:

```bash
docker compose logs -f airflow
```

Airflow ist anschließend erreichbar unter:

```text
http://localhost:8080
```

### Täglichen Generator-Batch ausführen

Beispiel für den 1. Januar 2026:

```bash
docker compose run --rm \
  --entrypoint python \
  airflow \
  /opt/airflow/src/generator/generate_transactions.py \
  --logical-date 2026-01-01 \
  --output-dir /opt/airflow/data
```

Die erzeugten Daten werden im Docker Named Volume `airflow_data` gespeichert und nach Datum partitioniert:

```text
/opt/airflow/data/bronze_raw/transactions/
└── year=2026/
    └── month=01/
        └── day=01/
            └── transactions.jsonl
```

### Erzeugte Batches anzeigen

```bash
docker compose run --rm \
  --entrypoint /bin/bash \
  airflow \
  -lc '
  find /opt/airflow/data/bronze_raw/transactions \
    -type f \
    -name "*.jsonl" \
    | sort
  '
```

### Services stoppen

```bash
docker compose down
```

Die Daten in den Named Volumes bleiben dabei erhalten.

### Services und lokale Docker-Daten vollständig entfernen

Achtung: Der folgende Befehl löscht auch die erzeugten Daten und die lokale Airflow-Datenbank:

```bash
docker compose down -v
```

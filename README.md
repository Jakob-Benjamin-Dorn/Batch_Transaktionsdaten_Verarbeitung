# Batch-Transaktionsdaten-Verarbeitung

Dieses Portfolio-Projekt simuliert eine tägliche Batch-Datenpipeline für synthetische Finanztransaktionen.

Die Pipeline:

1. erzeugt reproduzierbare tägliche Rohdaten,
2. validiert und bereinigt sie mit Apache Spark,
3. trennt gültige und fehlerhafte Datensätze,
4. erstellt aggregierte Business-Kennzahlen,
5. erzeugt später aus ausschließlich aggregierten Daten eine KI-gestützte Zusammenfassung,
6. wird mit Apache Airflow orchestriert.

## Architektur

```text
Synthetic Data Generator
        ↓
Bronze: rohe JSONL-Transaktionen
        ↓
Spark Data Quality und Deduplizierung
        ├── Silver: gültige, typisierte Parquet-Daten
        └── Quarantine: ungültige Zeilen mit DQ-Regeln
        ↓
Gold: aggregierte Business-Kennzahlen
        ↓
KI-Zusammenfassung aus freigegebenen Metriken
```

Die Laufzeitumgebung wird vollständig mit Docker bereitgestellt. Eine lokale Python-, Java-, Spark- oder Airflow-Installation ist nicht notwendig.

## Docker-Konzept

Der Quellcode wird read-only in die Container eingebunden. Veränderliche Laufzeitdaten werden in Docker Named Volumes gespeichert.

```text
Bind-Mounts, read-only:
- src/
- tests/
- airflow/dags/
- knowledge_base/

Named Volumes:
- Bronze-, Silver-, Quarantine- und Gold-Daten
- Airflow-Datenbank
- Airflow-Logs
```

Ein Initialisierungs-Service richtet die benötigten Volumes und Berechtigungen ein. Dadurch ist keine Anpassung an eine Linux-Host-UID erforderlich.

## Voraussetzungen

* Git
* Docker
* Docker Compose

## Repository klonen

```bash
git clone https://github.com/Jakob-Benjamin-Dorn/Batch_Transaktionsdaten_Verarbeitung.git
cd Batch_Transaktionsdaten_Verarbeitung
```

## Services starten

Beim ersten Start oder nach Änderungen an `Dockerfile` beziehungsweise `requirements.txt`:

```bash
docker compose up --build -d
```

Bei späteren Starts:

```bash
docker compose up -d
```

Status prüfen:

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

## 1. Täglichen Bronze-Batch erzeugen

Beispiel für den 1. Januar 2026:

```bash
docker compose run --rm \
  --entrypoint python \
  airflow \
  /opt/airflow/src/generator/generate_transactions.py \
  --logical-date 2026-01-01 \
  --output-dir /opt/airflow/data
```

Die Rohdaten werden als partitionierte JSONL-Datei gespeichert:

```text
/opt/airflow/data/bronze_raw/transactions/
└── year=2026/
    └── month=01/
        └── day=01/
            └── transactions.jsonl
```

Der Generator erzeugt bewusst unter anderem:

* Duplikate,
* fehlende Kunden-IDs,
* ungültige Währungen,
* beschädigte und zukünftige Zeitstempel,
* fehlerhafte Purchase- und Refund-Beträge,
* verspätet eintreffende Events,
* simulierte Schema Drift.

Für dasselbe Datum wird durch einen datumsabhängigen Seed immer derselbe Batch erzeugt.

## 2. Silver- und Quarantine-Layer erzeugen

```bash
docker compose run --rm \
  --entrypoint python \
  airflow \
  /opt/airflow/src/spark/transform_transactions.py \
  --logical-date 2026-01-01 \
  --data-dir /opt/airflow/data
```

Spark liest die Bronze-Daten mit einem expliziten Schema ein und wendet die Data-Quality-Regeln aus dem Projektkontext an.

Gültige und deduplizierte Datensätze werden im Silver-Layer gespeichert:

```text
/opt/airflow/data/silver_processed/transactions/
```

Fehlerhafte Datensätze werden zusammen mit ihren Regelverletzungen im Quarantine-Layer gespeichert:

```text
/opt/airflow/data/quarantine/transactions/
```

Zusätzlich entsteht ein technischer Qualitätsbericht:

```text
/opt/airflow/data/reports/year=2026/month=01/day=01/quality_report.json
```

## 3. Gold-Layer erzeugen

```bash
docker compose run --rm \
  --entrypoint python \
  airflow \
  /opt/airflow/src/spark/build_gold.py \
  --logical-date 2026-01-01 \
  --data-dir /opt/airflow/data
```

Der Gold-Job berechnet unter anderem:

* Anzahl aller gültigen Transaktionen,
* Approval-, Fraud- und Refund-Raten,
* Purchase-, Refund- und Nettobeträge,
* durchschnittlichen genehmigten Purchase-Betrag,
* Länder- und Währungsaggregate.

Die analytischen Daten werden als Parquet gespeichert:

```text
/opt/airflow/data/gold_analytics/daily_kpis/
/opt/airflow/data/gold_analytics/country_currency_summary/
```

Zusätzlich wird eine kompakte JSON-Datei für nachgelagerte Anwendungen erzeugt:

```text
/opt/airflow/data/reports/year=2026/month=01/day=01/business_metrics.json
```

Die spätere KI-Komponente erhält nur diese aggregierten Kennzahlen und den aggregierten Qualitätsbericht. Einzelne Kunden-, Händler- oder Transaktionsdatensätze werden nicht an die externe API übertragen.

## Tests ausführen

Alle Unit- und Integrationstests:

```bash
docker compose run --rm \
  --entrypoint python \
  airflow \
  -m unittest discover \
  -s /opt/airflow/tests \
  -p "test_*.py" \
  -v
```

Die Tests prüfen unter anderem:

* Reproduzierbarkeit des Generators,
* partitionierte JSONL-Ausgabe,
* Schema Drift,
* einzelne Data-Quality-Regeln,
* Deduplizierung,
* Aufteilung in Silver und Quarantine,
* Berechnung der Gold-Kennzahlen.

## Ergebnisse anzeigen

Alle Laufzeitdateien:

```bash
docker compose run --rm \
  --entrypoint /bin/bash \
  airflow \
  -lc '
find /opt/airflow/data -type f | sort
'
```

Qualitätsbericht:

```bash
docker compose run --rm \
  --entrypoint /bin/bash \
  airflow \
  -lc '
cat /opt/airflow/data/reports/year=2026/month=01/day=01/quality_report.json
'
```

Business-Kennzahlen:

```bash
docker compose run --rm \
  --entrypoint /bin/bash \
  airflow \
  -lc '
cat /opt/airflow/data/reports/year=2026/month=01/day=01/business_metrics.json
'
```

Verteilung der Quarantine-Regeln:

```bash
docker compose run --rm \
  --entrypoint /bin/bash \
  airflow \
  -lc '
python - <<'"'"'PY'"'"'
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = (
    SparkSession.builder
    .appName("inspect-quarantine")
    .master("local[2]")
    .getOrCreate()
)

path = (
    "/opt/airflow/data/quarantine/transactions/"
    "year=2026/month=01/day=01"
)

quarantine = spark.read.parquet(path)

(
    quarantine
    .select(explode("dq_errors").alias("dq_rule"))
    .groupBy("dq_rule")
    .count()
    .orderBy("dq_rule")
    .show(truncate=False)
)

spark.stop()
PY
'
```

## Services stoppen

```bash
docker compose down
```

Die Named Volumes bleiben dabei erhalten.

Um auch alle erzeugten Daten, Logs und die lokale Airflow-Datenbank zu löschen:

```bash
docker compose down -v
```

## Aktueller Entwicklungsstand

Implementiert:

* Docker-Laufzeitumgebung
* täglicher synthetischer Datengenerator
* partitionierter Bronze-Layer
* explizites Spark-Schema
* Data Contract
* acht Data-Quality-Regeln
* Silver- und Quarantine-Layer
* tägliche Gold-Kennzahlen
* automatisierte Unit- und Integrationstests

Geplant:

* KI-gestützte Zusammenfassung mit strukturiertem JSON-Output
* deterministisches Markdown-Rendering
* vollständige Airflow-Orchestrierung
* Quality Gate für kritische Pipeline-Fehler

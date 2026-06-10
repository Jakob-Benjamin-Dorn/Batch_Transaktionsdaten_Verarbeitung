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

* zwei getrennte Airflow-DAGs
* täglicher Core-DAG
* deterministisches Quality Gate
* task-spezifische Retries
* separater KI-Reporting-DAG
* automatische Übergabe des Batch-Datums
* strukturierter DeepSeek-Report
* deterministisches Markdown-Rendering
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

- optionaler Vergleich mehrerer KI-Provider
- PostgreSQL statt SQLite für ein produktionsnäheres Airflow-Setup
- CI-Pipeline für automatisierte Tests
- optionale Alerting- und Monitoring-Erweiterungen

## Airflow-Orchestrierung

Die Pipeline wird durch zwei getrennte Airflow-DAGs orchestriert.

### Core-DAG

```text
batch_transaction_pipeline
```

Der Core-DAG enthält ausschließlich die deterministischen Verarbeitungsschritte:

```text
generate_bronze
        ↓
transform_to_silver
        ↓
evaluate_quality_gate
        ↓
build_gold
        ↓
prepare_ai_payload
        ↓
trigger_ai_reporting_pipeline
```

Der DAG erzeugt:

* den partitionierten Bronze-Batch,
* bereinigte und deduplizierte Silver-Daten,
* Quarantine-Daten mit Data-Quality-Fehlern,
* den technischen Quality Report,
* die Gold-Aggregationen,
* das datenschutzgeprüfte AI-Payload.

Der Core-DAG ist nicht vom Erfolg der externen KI-API abhängig. Nachdem der KI-DAG erfolgreich ausgelöst wurde, kann der Core-DAG als erfolgreich abgeschlossen werden.

### KI-Reporting-DAG

```text
ai_reporting_pipeline
```

Der separate KI-DAG wird vom Core-DAG ausgelöst und besteht aus:

```text
create_ai_summary
        ↓
render_markdown
```

Der erste Task sendet ausschließlich aggregierte und freigegebene Kennzahlen an die DeepSeek-API. Die strukturierte Antwort wird lokal mit Pydantic validiert.

Der zweite Task erzeugt anschließend deterministisch einen Markdown-Bericht aus dem validierten JSON.

Ein Ausfall der externen API betrifft damit nur den KI-Reporting-DAG. Die zuvor erzeugten Bronze-, Silver-, Quarantine- und Gold-Daten bleiben erhalten, und der Core-DAG kann weiterhin erfolgreich sein.

## Retry-Verhalten

Retries werden nur für Schritte verwendet, bei denen ein erneuter Versuch sinnvoll sein kann.

| Task                            | Retries | Begründung                                   |
| ------------------------------- | ------: | -------------------------------------------- |
| `generate_bronze`               |       1 | Temporärer Dateisystemfehler möglich         |
| `transform_to_silver`           |       1 | Temporäre Spark- oder JVM-Probleme möglich   |
| `evaluate_quality_gate`         |       0 | Deterministische Entscheidung                |
| `build_gold`                    |       1 | Temporäre Spark-Probleme möglich             |
| `prepare_ai_payload`            |       0 | Deterministische lokale Verarbeitung         |
| `trigger_ai_reporting_pipeline` |       0 | Reiner Orchestrierungsschritt                |
| `create_ai_summary`             |       3 | Externe API, Netzwerkfehler oder Rate Limits |
| `render_markdown`               |       0 | Deterministisches lokales Rendering          |

Der DeepSeek-Task verwendet einen exponentiellen Retry-Abstand. Ein vorübergehender API-Ausfall führt deshalb zunächst zu einem erneuten Versuch und nicht unmittelbar zu einem endgültigen Fehler.

## Quality Gate

Nach der Spark-Transformation wird der technische Quality Report gegen konfigurierbare Schwellenwerte aus dem Data Contract geprüft.

Aktuelle Beispielkonfiguration:

```json
{
  "quality_gate": {
    "minimum_validity_rate": 0.85,
    "maximum_high_severity_failure_rate": 0.1
  }
}
```

Der Core-DAG wird gestoppt, wenn:

* der Batch keine Eingabezeilen enthält,
* die Validitätsrate unter 85 Prozent fällt,
* eine einzelne High-Severity-Regel mehr als 10 Prozent der Eingabezeilen betrifft.

Die Schwellenwerte dienen als nachvollziehbare Beispielwerte für dieses Portfolio-Projekt und stellen keine allgemeinen Branchenstandards dar.

## DAGs aktivieren

Beide DAGs können über die Airflow-Oberfläche aktiviert werden:

```text
http://localhost:8080
```

Alternativ über die Kommandozeile:

```bash
docker compose exec airflow \
  airflow dags unpause batch_transaction_pipeline
```

```bash
docker compose exec airflow \
  airflow dags unpause ai_reporting_pipeline
```

## Core-DAG manuell starten

```bash
docker compose exec airflow \
  airflow dags trigger \
  --logical-date 2026-01-02T00:00:00+00:00 \
  batch_transaction_pipeline
```

Die logische Zeit beschreibt das zu verarbeitende Datenintervall und nicht zwingend den tatsächlichen Startzeitpunkt des Runs.

## DAG-Runs kontrollieren

Core-DAG:

```bash
docker compose exec airflow \
  airflow dags list-runs \
  batch_transaction_pipeline
```

KI-Reporting-DAG:

```bash
docker compose exec airflow \
  airflow dags list-runs \
  ai_reporting_pipeline
```

In der verwendeten Airflow-Version wird die DAG-ID bei `list-runs` als Positionsargument angegeben.

## Task-Zustände eines Runs anzeigen

```bash
docker compose exec airflow \
  airflow tasks states-for-dag-run \
  ai_reporting_pipeline \
  "<RUN_ID>"
```

Die Run-ID kann zuvor aus `airflow dags list-runs` übernommen werden.

## DAG-Importfehler prüfen

```bash
docker compose exec airflow \
  airflow dags list-import-errors
```

## Historischen Core-DAG testen

```bash
docker compose exec airflow \
  airflow dags test \
  batch_transaction_pipeline \
  2026-01-01
```

Dieser Test führt die Core-Verarbeitung für den angegebenen logischen Tag aus und löst anschließend den KI-Reporting-DAG aus.

## Ergebnisdateien

Für einen vollständig erfolgreichen Lauf entstehen im Reports-Verzeichnis:

```text
/opt/airflow/data/reports/year=2026/month=01/day=01/
├── quality_report.json
├── business_metrics.json
├── ai_input_payload.json
├── ai_summary.json
└── ai_summary.md
```

Dateien anzeigen:

```bash
docker compose run --rm \
  --entrypoint /bin/bash \
  airflow \
  -lc '
find \
  /opt/airflow/data/reports/year=2026/month=01/day=01 \
  -maxdepth 1 \
  -type f \
  -printf "%f\n" \
  | sort
'
```

Markdown-Bericht anzeigen:

```bash
docker compose run --rm \
  --entrypoint /bin/bash \
  airflow \
  -lc '
sed -n "1,220p" \
  /opt/airflow/data/reports/year=2026/month=01/day=01/ai_summary.md
'
```

## Fehlerverhalten

Das Projekt unterscheidet bewusst zwischen Datenverarbeitung und externer KI-Berichterstellung.

```text
Spark- oder Quality-Gate-Fehler
→ Core-DAG schlägt fehl

DeepSeek-API vorübergehend nicht erreichbar
→ KI-Task wird erneut versucht

DeepSeek-API dauerhaft nicht erreichbar
→ KI-DAG schlägt fehl
→ Core-DAG und Datenprodukte bleiben erfolgreich erhalten
```

Diese Trennung verhindert, dass eine nicht verfügbare externe API die eigentliche Datenpipeline als fehlgeschlagen erscheinen lässt.

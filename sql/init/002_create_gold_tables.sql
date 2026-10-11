-- Serving layer: Gold-Aggregationen pro Batch-Tag.
-- Alle Statements sind idempotent (IF NOT EXISTS / OR REPLACE), weil der Loader
-- sie bei jedem Lauf ausführt. So funktioniert das Setup auch mit einem
-- bereits existierenden Postgres-Volume, bei dem init-Skripte nicht erneut laufen.

CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS analytics.daily_kpis (
    logical_date                      date PRIMARY KEY,
    total_transactions                bigint NOT NULL CHECK (total_transactions >= 0),
    approved_transactions             bigint,
    declined_transactions             bigint,
    pending_transactions              bigint,
    purchase_transactions             bigint,
    refund_transactions               bigint,
    fraud_transactions                bigint,
    late_arriving_transactions        bigint,
    total_purchase_amount             numeric(18, 2),
    total_refund_amount               numeric(18, 2),
    net_amount                        numeric(18, 2),
    average_approved_purchase_amount  numeric(18, 2),
    fraud_rate                        numeric(6, 4) CHECK (fraud_rate BETWEEN 0 AND 1),
    refund_rate                       numeric(6, 4) CHECK (refund_rate BETWEEN 0 AND 1),
    approval_rate                     numeric(6, 4) CHECK (approval_rate BETWEEN 0 AND 1),
    loaded_at                         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.country_currency_summary (
    logical_date         date NOT NULL,
    transaction_country  text NOT NULL,
    currency             text NOT NULL,
    transaction_count    bigint NOT NULL CHECK (transaction_count >= 0),
    approved_count       bigint,
    fraud_count          bigint,
    net_amount           numeric(18, 2),
    loaded_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (logical_date, transaction_country, currency)
);

-- Beispiel für eine Mart-Sicht: gleitender 7-Tage-Durchschnitt über Kalendertage.
-- RANGE statt ROWS, damit Lücken (z.B. nicht nachgeholte Tage) das Fenster
-- nicht verfälschen.
CREATE OR REPLACE VIEW marts.daily_kpi_trend AS
SELECT
    logical_date,
    total_transactions,
    net_amount,
    approval_rate,
    fraud_rate,
    round(avg(total_transactions) OVER w, 1) AS transactions_7d_avg,
    round(avg(fraud_rate) OVER w, 4)         AS fraud_rate_7d_avg
FROM analytics.daily_kpis
WINDOW w AS (
    ORDER BY logical_date
    RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
);

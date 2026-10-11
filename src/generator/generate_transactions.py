from __future__ import annotations

import argparse
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.dates import validate_logical_date

# -----------------------------
# Konfiguration
# -----------------------------

@dataclass
class Config:
    seed: int = 42
    output_dir: str = "data"

    n_customers: int = 2_000
    n_merchants: int = 250
    n_days: int = 1
    avg_transactions_per_day: int = 2_500

    start_date: str = "2026-01-01"
    schema_drift_start_date: str = "2026-01-20"

    duplicate_rate: float = 0.03
    missing_customer_rate: float = 0.02
    invalid_currency_rate: float = 0.01
    bad_timestamp_rate: float = 0.01
    negative_amount_error_rate: float = 0.005
    late_arrival_rate: float = 0.04


COUNTRIES = {
    "DE": {"currency": "EUR", "cities": ["Berlin", "Hamburg", "München", "Köln", "Frankfurt"]},
    "FR": {"currency": "EUR", "cities": ["Paris", "Lyon", "Marseille", "Toulouse"]},
    "NL": {"currency": "EUR", "cities": ["Amsterdam", "Rotterdam", "Utrecht"]},
    "GB": {"currency": "GBP", "cities": ["London", "Manchester", "Birmingham"]},
    "US": {"currency": "USD", "cities": ["New York", "San Francisco", "Chicago", "Austin"]},
    "PL": {"currency": "PLN", "cities": ["Warschau", "Krakau", "Gdansk"]},
}

MERCHANT_CATEGORIES = {
    "grocery": {"mean": 38, "std": 18, "risk": 0.01},
    "electronics": {"mean": 260, "std": 140, "risk": 0.04},
    "travel": {"mean": 420, "std": 220, "risk": 0.035},
    "gaming": {"mean": 55, "std": 35, "risk": 0.025},
    "fashion": {"mean": 85, "std": 55, "risk": 0.02},
    "crypto": {"mean": 600, "std": 400, "risk": 0.08},
    "restaurant": {"mean": 42, "std": 25, "risk": 0.015},
}

PAYMENT_METHODS = ["card", "paypal", "bank_transfer", "apple_pay", "google_pay"]


# -----------------------------
# Hilfsfunktionen
# -----------------------------

def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)


def seed_from_date(logical_date: str, base_seed: int = 42) -> int:
    """Erzeugt für jedes Datum einen stabilen, reproduzierbaren Seed."""
    value = f"{logical_date}:{base_seed}".encode()
    digest = hashlib.sha256(value).hexdigest()
    return int(digest[:8], 16)


def random_id(prefix: str) -> str:
    """Erzeugt eine durch random.seed() reproduzierbare ID."""
    return f"{prefix}_{random.getrandbits(48):012x}"


def weighted_choice(items: list[str], weights: list[float]) -> str:
    return random.choices(items, weights=weights, k=1)[0]


def positive_normal(mean: float, std: float, minimum: float = 1.0) -> float:
    value = np.random.normal(mean, std)
    return round(float(max(value, minimum)), 2)


def random_timestamp_for_day(day: datetime) -> datetime:
    seconds = random.randint(0, 24 * 60 * 60 - 1)
    return day + timedelta(seconds=seconds)


# -----------------------------
# Stammdaten erzeugen
# -----------------------------

def generate_customers(cfg: Config) -> pd.DataFrame:
    rows = []

    country_codes = list(COUNTRIES.keys())
    country_weights = [0.30, 0.15, 0.10, 0.15, 0.20, 0.10]

    for _ in range(cfg.n_customers):
        country = weighted_choice(country_codes, country_weights)
        city = random.choice(COUNTRIES[country]["cities"])
        currency = COUNTRIES[country]["currency"]

        # Kundentyp beeinflusst spätere Transaktionen
        risk_segment = weighted_choice(
            ["low", "medium", "high"],
            [0.75, 0.20, 0.05],
        )

        age = int(np.random.normal(38, 12))
        age = max(18, min(age, 85))

        rows.append({
            "customer_id": random_id("cus"),
            "home_country": country,
            "home_city": city,
            "preferred_currency": currency,
            "risk_segment": risk_segment,
            "age": age,
            "created_at": datetime.fromisoformat(cfg.start_date) - timedelta(days=random.randint(1, 900)),
        })

    return pd.DataFrame(rows)


def generate_merchants(cfg: Config) -> pd.DataFrame:
    rows = []

    country_codes = list(COUNTRIES.keys())
    categories = list(MERCHANT_CATEGORIES.keys())

    for _ in range(cfg.n_merchants):
        country = random.choice(country_codes)
        category = random.choice(categories)

        rows.append({
            "merchant_id": random_id("mer"),
            "merchant_name": f"{category}_{random.getrandbits(24):06x}",
            "merchant_category": category,
            "merchant_country": country,
            "merchant_city": random.choice(COUNTRIES[country]["cities"]),
            "merchant_currency": COUNTRIES[country]["currency"],
            "base_risk": MERCHANT_CATEGORIES[category]["risk"],
        })

    return pd.DataFrame(rows)


def generate_devices(customers: pd.DataFrame) -> pd.DataFrame:
    rows = []

    device_types = ["ios", "android", "web", "tablet"]
    device_weights = [0.35, 0.35, 0.25, 0.05]

    for customer_id in customers["customer_id"]:
        n_devices = random.choices([1, 2, 3], weights=[0.65, 0.28, 0.07], k=1)[0]

        for i in range(n_devices):
            device_type = weighted_choice(device_types, device_weights)
            rows.append({
                "device_id": random_id("dev"),
                "customer_id": customer_id,
                "device_type": device_type,
                "is_primary_device": i == 0,
                "device_created_at": datetime.fromisoformat("2025-01-01") + timedelta(days=random.randint(0, 365)),
            })

    return pd.DataFrame(rows)


def generate_exchange_rates(cfg: Config) -> pd.DataFrame:
    start = datetime.fromisoformat(cfg.start_date)
    currencies = ["EUR", "USD", "GBP", "PLN"]

    base_rates_to_eur = {
        "EUR": 1.00,
        "USD": 0.92,
        "GBP": 1.17,
        "PLN": 0.23,
    }

    rows = []
    for day_idx in range(cfg.n_days):
        day = start + timedelta(days=day_idx)

        for currency in currencies:
            noise = np.random.normal(0, 0.01)
            rate = base_rates_to_eur[currency] * (1 + noise)

            rows.append({
                "date": day.date().isoformat(),
                "currency": currency,
                "rate_to_eur": round(float(rate), 4),
            })

    return pd.DataFrame(rows)


# -----------------------------
# Transaktionslogik
# -----------------------------

def compute_fraud_probability(
    customer: pd.Series,
    merchant: pd.Series,
    amount: float,
    transaction_country: str,
    device_is_primary: bool,
    hour: int,
) -> float:
    probability = merchant["base_risk"]

    if customer["risk_segment"] == "medium":
        probability += 0.02
    elif customer["risk_segment"] == "high":
        probability += 0.08

    if transaction_country != customer["home_country"]:
        probability += 0.025

    if not device_is_primary:
        probability += 0.015

    if amount > 500:
        probability += 0.025

    if hour < 5:
        probability += 0.02

    if merchant["merchant_category"] == "crypto":
        probability += 0.04

    return min(probability, 0.85)


def generate_transactions(
    cfg: Config,
    customers: pd.DataFrame,
    merchants: pd.DataFrame,
    devices: pd.DataFrame,
) -> pd.DataFrame:
    start = datetime.fromisoformat(cfg.start_date)
    rows = []

    customers_by_id = customers.set_index("customer_id")
    merchants_by_id = merchants.set_index("merchant_id")

    devices_by_customer = {
        customer_id: group.to_dict("records")
        for customer_id, group in devices.groupby("customer_id")
    }

    customer_ids = customers["customer_id"].tolist()
    merchant_ids = merchants["merchant_id"].tolist()

    for day_idx in range(cfg.n_days):
        current_day = start + timedelta(days=day_idx)

        # Wochenenden leicht anderes Volumen
        weekday = current_day.weekday()
        weekend_multiplier = 1.25 if weekday >= 5 else 1.0

        n_transactions = int(np.random.poisson(cfg.avg_transactions_per_day * weekend_multiplier))

        for _ in range(n_transactions):
            customer_id = random.choice(customer_ids)
            merchant_id = random.choice(merchant_ids)

            customer = customers_by_id.loc[customer_id]
            merchant = merchants_by_id.loc[merchant_id]

            category_config = MERCHANT_CATEGORIES[merchant["merchant_category"]]

            # Kunde kauft meistens im Heimatland, manchmal international
            if random.random() < 0.85:
                transaction_country = customer["home_country"]
                transaction_city = customer["home_city"]
                currency = customer["preferred_currency"]
            else:
                transaction_country = random.choice(list(COUNTRIES.keys()))
                transaction_city = random.choice(COUNTRIES[transaction_country]["cities"])
                currency = COUNTRIES[transaction_country]["currency"]

            amount = positive_normal(
                category_config["mean"],
                category_config["std"],
                minimum=2.0,
            )

            # High-risk Kunden haben gelegentlich höhere Beträge
            if customer["risk_segment"] == "high" and random.random() < 0.20:
                amount *= random.uniform(2.0, 5.0)
                amount = round(amount, 2)

            customer_devices = devices_by_customer[customer_id]
            device = random.choice(customer_devices)

            timestamp = random_timestamp_for_day(current_day)
            payment_method = random.choice(PAYMENT_METHODS)

            fraud_probability = compute_fraud_probability(
                customer=customer,
                merchant=merchant,
                amount=amount,
                transaction_country=transaction_country,
                device_is_primary=device["is_primary_device"],
                hour=timestamp.hour,
            )

            is_fraud = random.random() < fraud_probability

            # Zahlungsstatus
            if is_fraud and random.random() < 0.18:
                status = "declined"
            else:
                status = weighted_choice(
                    ["approved", "declined", "pending"],
                    [0.93, 0.05, 0.02],
                )

            is_refund = False
            transaction_type = "purchase"

            # Manche approved purchases werden Refunds
            if status == "approved" and random.random() < 0.025:
                is_refund = True
                transaction_type = "refund"
                amount = -amount

            event_time = timestamp

            # ingestion_time kann verspätet sein
            if random.random() < cfg.late_arrival_rate:
                ingestion_time = event_time + timedelta(days=random.randint(1, 4), minutes=random.randint(0, 180))
                is_late_arriving = True
            else:
                ingestion_time = event_time + timedelta(minutes=random.randint(1, 90))
                is_late_arriving = False

            row = {
                "transaction_id": random_id("txn"),
                "customer_id": customer_id,
                "merchant_id": merchant_id,
                "device_id": device["device_id"],
                "event_time": event_time,
                "ingestion_time": ingestion_time,
                "amount": round(float(amount), 2),
                "currency": currency,
                "transaction_country": transaction_country,
                "transaction_city": transaction_city,
                "payment_method": payment_method,
                "transaction_type": transaction_type,
                "status": status,
                "is_refund": is_refund,
                "is_fraud": is_fraud,
                "is_late_arriving": is_late_arriving,
                "source_system": weighted_choice(
                    ["payment_gateway_v1", "payment_gateway_v2", "mobile_app", "web_checkout"],
                    [0.40, 0.25, 0.20, 0.15],
                ),
            }

            # Simulierte Schema-Drift: ab Tag 20 taucht ein neues Feld auf
            schema_drift_start = datetime.fromisoformat(
                cfg.schema_drift_start_date
            )

            if current_day >= schema_drift_start:
                row["device_trust_score"] = round(
                    random.uniform(0.1, 1.0),
                    3,
                )

            rows.append(row)

    return pd.DataFrame(rows)


def generate_chargebacks(transactions: pd.DataFrame) -> pd.DataFrame:
    candidates = transactions[
        (transactions["status"] == "approved")
        & (transactions["transaction_type"] == "purchase")
        & (transactions["amount"] > 0)
    ].copy()

    rows = []

    for _, txn in candidates.iterrows():
        base_prob = 0.003

        if txn["is_fraud"]:
            base_prob += 0.08

        if txn["amount"] > 500:
            base_prob += 0.01

        if txn["source_system"] == "payment_gateway_v2":
            base_prob += 0.004

        if random.random() < base_prob:
            event_time = txn["event_time"] + timedelta(days=random.randint(3, 25))
            rows.append({
                "chargeback_id": random_id("cbk"),
                "original_transaction_id": txn["transaction_id"],
                "customer_id": txn["customer_id"],
                "merchant_id": txn["merchant_id"],
                "chargeback_time": event_time,
                "chargeback_amount": txn["amount"],
                "currency": txn["currency"],
                "reason_code": weighted_choice(
                    ["fraud", "product_not_received", "duplicate_charge", "customer_dispute"],
                    [0.45, 0.25, 0.15, 0.15],
                ),
            })

    return pd.DataFrame(rows)


# -----------------------------
# Dirty Data erzeugen
# -----------------------------

def inject_data_quality_issues(cfg: Config, transactions: pd.DataFrame) -> pd.DataFrame:
    dirty = transactions.copy()
    # Bronze-Rohdaten dürfen gemischte Datentypen enthalten.
    # Dadurch können wir absichtlich beschädigte Zeitstempel injizieren.
    dirty["event_time"] = dirty["event_time"].astype("object")

    n = len(dirty)

    # 1. Duplikate
    n_duplicates = int(n * cfg.duplicate_rate)
    duplicate_rows = dirty.sample(n=n_duplicates, random_state=cfg.seed).copy()

    # Leicht andere ingestion_time, aber gleiche transaction_id
    duplicate_rows["ingestion_time"] = duplicate_rows["ingestion_time"] + pd.to_timedelta(
        np.random.randint(1, 360, size=n_duplicates),
        unit="m",
    )

    dirty = pd.concat([dirty, duplicate_rows], ignore_index=True)

    # 2. Fehlende customer_id
    idx = dirty.sample(frac=cfg.missing_customer_rate, random_state=cfg.seed + 1).index
    dirty.loc[idx, "customer_id"] = None

    # 3. Ungültige Währung
    idx = dirty.sample(frac=cfg.invalid_currency_rate, random_state=cfg.seed + 2).index
    dirty.loc[idx, "currency"] = random.choice(["EURO", "US", "XXX", "", None])

    # 4. Kaputte Zeitstempel als Strings
    idx = dirty.sample(frac=cfg.bad_timestamp_rate, random_state=cfg.seed + 3).index

    # Bronze speichert Zeitwerte als rohe Strings, damit auch
    # absichtlich ungültige Werte erhalten bleiben können.
    dirty["event_time"] = dirty["event_time"].map(
        lambda value: value.isoformat() if pd.notna(value) else None
    )
    dirty.loc[idx, "event_time"] = "not-a-timestamp"

    # 5. Negative Beträge ohne Refund-Flag
    idx = dirty[
        (dirty["amount"] > 0)
        & (dirty["transaction_type"] == "purchase")
    ].sample(frac=cfg.negative_amount_error_rate, random_state=cfg.seed + 4).index

    dirty.loc[idx, "amount"] = dirty.loc[idx, "amount"] * -1
    dirty.loc[idx, "is_refund"] = False

    # 6. Future timestamps
    idx = dirty.sample(frac=0.003, random_state=cfg.seed + 5).index
    dirty.loc[idx, "event_time"] = "2035-01-01T00:00:00"

    # 7. Whitespace / uneinheitliche Source-Systeme
    idx = dirty.sample(frac=0.01, random_state=cfg.seed + 6).index
    dirty.loc[idx, "source_system"] = dirty.loc[idx, "source_system"].astype(str).str.upper() + " "

    return dirty.sample(frac=1.0, random_state=cfg.seed).reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one daily batch of synthetic transaction data."
    )

    parser.add_argument(
        "--logical-date",
        required=True,
        help="Batch date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--output-dir",
        default="data",
        help="Project data directory.",
    )

    return parser.parse_args()


def save_bronze_batch(
    cfg: Config,
    transactions_dirty: pd.DataFrame,
) -> Path:
    batch_date = datetime.fromisoformat(cfg.start_date)

    output_path = (
        Path(cfg.output_dir)
        / "bronze_raw"
        / "transactions"
        / f"year={batch_date.year:04d}"
        / f"month={batch_date.month:02d}"
        / f"day={batch_date.day:02d}"
        / "transactions.jsonl"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    transactions_dirty.to_json(
        output_path,
        orient="records",
        lines=True,
        date_format="iso",
    )

    print(f"Bronze batch written to: {output_path.resolve()}")
    print(f"Rows written: {len(transactions_dirty):,}")

    return output_path


def main() -> None:
    args = parse_args()

    validate_logical_date(args.logical_date)

    cfg = Config(
        start_date=args.logical_date,
        output_dir=args.output_dir,
        n_days=1,
    )

    daily_seed = seed_from_date(
        logical_date=args.logical_date,
        base_seed=cfg.seed,
    )
    set_seed(daily_seed)

    customers = generate_customers(cfg)
    merchants = generate_merchants(cfg)
    devices = generate_devices(customers)
    exchange_rates = generate_exchange_rates(cfg)

    transactions_clean = generate_transactions(
        cfg=cfg,
        customers=customers,
        merchants=merchants,
        devices=devices,
    )

    chargebacks = generate_chargebacks(transactions_clean)

    transactions_dirty = inject_data_quality_issues(
        cfg=cfg,
        transactions=transactions_clean,
    )

    save_bronze_batch(
        cfg=cfg,
        transactions_dirty=transactions_dirty,
    )

    print()
    print("Generated supporting entities:")
    print(f"customers:          {len(customers):,}")
    print(f"merchants:          {len(merchants):,}")
    print(f"devices:            {len(devices):,}")
    print(f"exchange rates:     {len(exchange_rates):,}")
    print(f"clean transactions: {len(transactions_clean):,}")
    print(f"dirty transactions: {len(transactions_dirty):,}")
    print(f"chargebacks:        {len(chargebacks):,}")


if __name__ == "__main__":
    main()

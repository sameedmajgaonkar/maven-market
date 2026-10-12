"""Event simulator: emits Orders and Inventory events to Confluent Kafka.

Orders/Inventory are not in the source dataset, so we simulate them. Crucially the keys
(customer_id / product_id / store_id) are sampled from the REAL dimension CSVs, so every event
joins to a dimension in Silver (the reason a random generator such as Datagen was rejected).

Layout: pure, unit-tested event builders + a thin Kafka runner (``run``) that needs
``confluent_kafka`` and credentials from env vars.
"""
from __future__ import annotations

import csv
import json
import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator


@dataclass(frozen=True)
class EventKeys:
    customer_ids: list[int]
    products: dict[int, float]  # product_id -> retail price
    store_ids: list[int]


def _read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def load_event_keys(keys_dir: str | Path, key_files: dict[str, str]) -> EventKeys:
    """Read the real customer/product/store ids from the dimension CSVs."""
    base = Path(keys_dir)
    customers = [int(r["customer_id"]) for r in _read_csv(base / key_files["customers"])]
    products = {
        int(r["product_id"]): float(r["product_retail_price"])
        for r in _read_csv(base / key_files["products"])
    }
    stores = [int(r["store_id"]) for r in _read_csv(base / key_files["stores"])]
    for name, values in (("customers", customers), ("products", products), ("stores", stores)):
        if not values:
            raise ValueError(f"No '{name}' keys found in {base}; cannot simulate events")
    return EventKeys(customer_ids=customers, products=products, store_ids=stores)


def build_order_event(keys: EventKeys, rng: random.Random, now: datetime) -> dict:
    product_id = rng.choice(list(keys.products))
    return {
        "event_type": "order",
        "order_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "customer_id": rng.choice(keys.customer_ids),
        "product_id": product_id,
        "store_id": rng.choice(keys.store_ids),
        "quantity": rng.randint(1, 5),
        "unit_price": keys.products[product_id],
        "order_ts": now.isoformat(),
    }


def build_inventory_event(keys: EventKeys, rng: random.Random, now: datetime) -> dict:
    return {
        "event_type": "inventory",
        "product_id": rng.choice(list(keys.products)),
        "store_id": rng.choice(keys.store_ids),
        "quantity_on_hand": rng.randint(0, 500),
        "event_ts": now.isoformat(),
    }


def iter_events(
    keys: EventKeys,
    seed: int = 42,
    max_events: int = 0,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    orders_ratio: float = 0.7,
) -> Iterator[tuple[str, dict]]:
    """Yield ``(topic, event)`` pairs. ``max_events=0`` means run forever."""
    rng = random.Random(seed)
    emitted = 0
    while max_events == 0 or emitted < max_events:
        if rng.random() < orders_ratio:
            yield "orders", build_order_event(keys, rng, now_fn())
        else:
            yield "inventory", build_inventory_event(keys, rng, now_fn())
        emitted += 1


def serialize(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def run(cfg: dict) -> None:  # pragma: no cover - needs a live Kafka cluster
    """Stream simulated events to Kafka at ``producer.events_per_second``.

    Credentials come from env vars (CONFLUENT_BOOTSTRAP / CONFLUENT_API_KEY / CONFLUENT_API_SECRET)
    when run locally, or from the Databricks secret scope when run as a job.
    """
    from confluent_kafka import Producer  # lazy: not available in plain unit-test envs

    pcfg = cfg["producer"]
    keys = load_event_keys(pcfg["keys_dir"], pcfg["key_files"])
    producer = Producer({
        "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONFLUENT_API_KEY"],
        "sasl.password": os.environ["CONFLUENT_API_SECRET"],
    })
    delay = 1.0 / max(float(pcfg["events_per_second"]), 0.001)
    for topic, event in iter_events(keys, seed=pcfg["seed"], max_events=pcfg["max_events"]):
        producer.produce(topic, value=serialize(event))
        producer.poll(0)
        time.sleep(delay)
    producer.flush()


if __name__ == "__main__":  # pragma: no cover
    from maven_market.config import load_config

    run(load_config(os.environ.get("MAVEN_ENV", "dev")))

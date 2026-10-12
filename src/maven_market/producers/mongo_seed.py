"""Seed MongoDB Atlas with the product master from the products CSV.

The capstone's NoSQL source is the Product master, so this loads the real product records into
``<mongo.database>.products``. Idempotent: documents are upserted by ``product_id``.

    pip install pymongo
    export MONGO_URI='mongodb+srv://<user>:<password>@<cluster>.mongodb.net/'   # never commit this
    python -m maven_market.producers.mongo_seed [--env dev] [--config-dir DIR]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from maven_market.producers.kafka_producer import _read_csv


def _num(value, cast):
    value = (value or "").strip()
    return cast(value) if value else None


def build_documents(rows: list[dict]) -> list[dict]:
    """CSV rows -> typed Mongo documents. Blank cells become None; rows without an id are skipped."""
    docs = []
    for r in rows:
        product_id = _num(r.get("product_id"), int)
        if product_id is None:
            continue
        docs.append({
            "product_id": product_id,
            "product_brand": (r.get("product_brand") or "").strip() or None,
            "product_name": (r.get("product_name") or "").strip() or None,
            "product_sku": _num(r.get("product_sku"), int),
            "product_retail_price": _num(r.get("product_retail_price"), float),
            "product_cost": _num(r.get("product_cost"), float),
            "product_weight": _num(r.get("product_weight"), float),
            "recyclable": _num(r.get("recyclable"), int),
            "low_fat": _num(r.get("low_fat"), int),
        })
    return docs


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - needs Atlas
    from pymongo import MongoClient, ReplaceOne

    from maven_market.config import get, load_config

    p = argparse.ArgumentParser(description="Seed Atlas with the product master")
    p.add_argument("--env", default="dev")
    p.add_argument("--config-dir", default=None)
    args = p.parse_args(argv if argv is not None else sys.argv[1:])

    uri = os.environ.get("MONGO_URI")
    if not uri:
        sys.exit("Set MONGO_URI first (export MONGO_URI='mongodb+srv://...'); it is not read from any file.")

    cfg = load_config(args.env, config_dir=args.config_dir)
    pcfg = cfg["producer"]
    docs = build_documents(_read_csv(Path(pcfg["keys_dir"]) / pcfg["key_files"]["products"]))

    client = MongoClient(uri, serverSelectionTimeoutMS=20000)
    collection = client[get(cfg, "mongo.database")]["products"]
    result = collection.bulk_write([ReplaceOne({"product_id": d["product_id"]}, d, upsert=True) for d in docs])
    collection.create_index("product_id", unique=True)
    print(
        f"[mongo_seed] {len(docs)} products -> {collection.full_name} "
        f"(inserted {result.upserted_count}, modified {result.modified_count}); "
        f"total now {collection.count_documents({})}"
    )
    client.close()


if __name__ == "__main__":  # pragma: no cover
    main()

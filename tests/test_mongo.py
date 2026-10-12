"""Pure-Python tests for the MongoDB product-master seed + ingest helpers (no Spark/Mongo needed)."""
import pytest

from maven_market.config import load_config
from maven_market.ingestion.mongo_ingest import (
    documents_to_rows,
    method,
    mongo_options,
    schema_columns,
)
from maven_market.jobs.bronze_mongo import parse_args, select_collections
from maven_market.producers.mongo_seed import build_documents


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


# ---- seed documents (CSV row -> typed Mongo document) ----

def test_build_documents_types_values_and_blanks():
    rows = [{
        "product_id": "1", "product_brand": "Washington", "product_name": "Berry Juice",
        "product_sku": "90748583674", "product_retail_price": "2.85", "product_cost": "0.94",
        "product_weight": "8.39", "recyclable": "", "low_fat": "1",
    }]
    doc = build_documents(rows)[0]
    assert doc["product_id"] == 1 and isinstance(doc["product_id"], int)
    assert doc["product_sku"] == 90748583674
    assert doc["product_retail_price"] == 2.85 and doc["product_weight"] == 8.39
    assert doc["recyclable"] is None and doc["low_fat"] == 1
    assert doc["product_name"] == "Berry Juice"


def test_build_documents_skips_rows_without_a_product_id():
    docs = build_documents([{"product_id": "", "product_name": "x"}, {"product_id": "2", "product_name": "y"}])
    assert [d["product_id"] for d in docs] == [2]


# ---- ingest helpers ----

def test_schema_columns_parses_ddl(cfg):
    ddl = cfg["mongo"]["collections"]["products"]["schema"]
    cols = schema_columns(ddl)
    assert cols[0] == "_id" and "product_id" in cols and "product_retail_price" in cols


def test_documents_to_rows_stringifies_id_and_fills_missing(cfg):
    cols = schema_columns(cfg["mongo"]["collections"]["products"]["schema"])
    rows = documents_to_rows([{"_id": object(), "product_id": 5}], cols)
    assert isinstance(rows[0]["_id"], str)
    assert rows[0]["product_id"] == 5
    assert rows[0]["product_cost"] is None            # absent field -> NULL, not KeyError
    assert set(rows[0]) == set(cols)                   # unknown extra fields are not leaked in


def test_method_defaults_to_pymongo_and_rejects_unknown(cfg):
    assert method(cfg) == "pymongo"
    with pytest.raises(ValueError):
        method({**cfg, "mongo": {**cfg["mongo"], "method": "carrier-pigeon"}})


def test_connector_options_still_built_from_config(cfg):
    o = mongo_options(cfg, "mongodb+srv://u:p@c.mongodb.net/", "products")
    assert (o["database"], o["collection"]) == ("maven_market", "products")


def test_unknown_collection_raises(cfg):
    with pytest.raises(KeyError):
        mongo_options(cfg, "mongodb+srv://x", "product_reviews")


# ---- job entry point ----

def test_job_args_and_collection_selection(cfg):
    assert parse_args([]).collections is None
    assert parse_args(["--collections", "products"]).collections == ["products"]
    assert select_collections(cfg, None) == ["products"]
    with pytest.raises(KeyError):
        select_collections(cfg, ["nope"])

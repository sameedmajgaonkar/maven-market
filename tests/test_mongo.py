"""Pure-Python tests for the MongoDB seed + ingest helpers (no Spark/Mongo needed)."""
from datetime import date, datetime

import pytest

from maven_market.config import load_config
from maven_market.ingestion.mongo_ingest import (
    documents_to_rows,
    method,
    mongo_options,
    schema_columns,
    schema_types,
)
from maven_market.jobs.bronze_mongo import parse_args, select_collections
from maven_market.producers.mongo_seed import build_documents


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


def _schema(cfg, name):
    return cfg["mongo"]["collections"][name]["schema"]


# ---- schema helpers ----

def test_schema_columns_and_types_parse_ddl(cfg):
    ddl = _schema(cfg, "customers")
    assert schema_columns(ddl)[0] == "_id"
    types = schema_types(ddl)
    assert types["birthdate"] == "DATE" and types["customer_id"] == "INT" and types["yearly_income"] == "STRING"


def test_every_collection_has_schema_table_id_field_and_a_csv_source(cfg):
    for name, spec in cfg["mongo"]["collections"].items():
        assert spec["bronze_table"].startswith("bronze.")
        assert spec["id_field"] in schema_columns(spec["schema"]), name
        assert name in cfg["producer"]["key_files"], f"no source CSV configured for '{name}'"


def test_both_master_collections_are_configured(cfg):
    assert set(cfg["mongo"]["collections"]) == {"products", "customers"}


# ---- seed documents: CSV row -> typed Mongo document, driven by the schema ----

def test_build_documents_products(cfg):
    rows = [{
        "product_id": "1", "product_brand": "Washington", "product_name": "Berry Juice",
        "product_sku": "90748583674", "product_retail_price": "2.85", "product_cost": "0.94",
        "product_weight": "8.39", "recyclable": "", "low_fat": "1",
    }]
    doc = build_documents(rows, _schema(cfg, "products"), "product_id")[0]
    assert doc["product_id"] == 1 and isinstance(doc["product_id"], int)
    assert doc["product_sku"] == 90748583674 and doc["product_retail_price"] == 2.85
    assert doc["recyclable"] is None and doc["low_fat"] == 1
    assert "_id" not in doc  # Mongo assigns it


def test_build_documents_customers_parses_us_dates_and_types(cfg):
    rows = [{
        "customer_id": "1", "customer_acct_num": "87462024688", "first_name": " Sheri ", "last_name": "Nowmer",
        "customer_address": "2433 Bailey Road", "customer_city": "Tlaxiaco", "customer_state_province": "Oaxaca",
        "customer_postal_code": "15057", "customer_country": "Mexico", "birthdate": "8/26/1961",
        "marital_status": "M", "yearly_income": "$30K - $50K", "gender": "F", "total_children": "4",
        "num_children_at_home": "2", "education": "Partial High School", "acct_open_date": "9/10/1991",
        "member_card": "Bronze", "occupation": "Skilled Manual", "homeowner": "Y",
    }]
    doc = build_documents(rows, _schema(cfg, "customers"), "customer_id")[0]
    assert doc["birthdate"] == datetime(1961, 8, 26) and doc["acct_open_date"] == datetime(1991, 9, 10)
    assert doc["customer_acct_num"] == 87462024688 and doc["total_children"] == 4
    assert doc["first_name"] == "Sheri"           # trimmed
    assert doc["yearly_income"] == "$30K - $50K"  # '$' preserved


def test_build_documents_skips_rows_without_id_and_blanks_become_none(cfg):
    rows = [{"customer_id": "", "first_name": "x"}, {"customer_id": "2", "birthdate": ""}]
    docs = build_documents(rows, _schema(cfg, "customers"), "customer_id")
    assert [d["customer_id"] for d in docs] == [2]
    assert docs[0]["birthdate"] is None and docs[0]["first_name"] is None


def test_build_documents_rejects_unsupported_type():
    with pytest.raises(ValueError):
        build_documents([{"id": "1", "x": "1"}], "id INT, x BINARY", "id")


# ---- ingest: documents -> rows ----

def test_documents_to_rows_stringifies_id_and_fills_missing(cfg):
    cols = schema_columns(_schema(cfg, "products"))
    rows = documents_to_rows([{"_id": object(), "product_id": 5}], cols)
    assert isinstance(rows[0]["_id"], str) and rows[0]["product_id"] == 5
    assert rows[0]["product_cost"] is None            # absent field -> NULL, not KeyError
    assert set(rows[0]) == set(cols)                   # unknown extra fields are not leaked in


def test_documents_to_rows_converts_mongo_datetimes_to_dates_for_date_columns(cfg):
    ddl = _schema(cfg, "customers")
    rows = documents_to_rows(
        [{"_id": 1, "customer_id": 1, "birthdate": datetime(1961, 8, 26), "acct_open_date": None}],
        schema_columns(ddl), schema_types(ddl),
    )
    assert rows[0]["birthdate"] == date(1961, 8, 26) and type(rows[0]["birthdate"]) is date
    assert rows[0]["acct_open_date"] is None


def test_documents_to_rows_extra_unknown_field_dropped(cfg):
    ddl = _schema(cfg, "products")
    rows = documents_to_rows([{"_id": 1, "product_id": 1, "surprise": "x"}], schema_columns(ddl), schema_types(ddl))
    assert "surprise" not in rows[0]


# ---- method switch / options ----

def test_method_defaults_to_pymongo_and_rejects_unknown(cfg):
    assert method(cfg) == "pymongo"
    with pytest.raises(ValueError):
        method({**cfg, "mongo": {**cfg["mongo"], "method": "carrier-pigeon"}})


def test_connector_options_built_from_config(cfg):
    o = mongo_options(cfg, "mongodb+srv://u:p@c.mongodb.net/", "customers")
    assert (o["database"], o["collection"]) == ("maven_market", "customers")


def test_unknown_collection_raises(cfg):
    with pytest.raises(KeyError):
        mongo_options(cfg, "mongodb+srv://x", "product_reviews")


# ---- job entry point ----

def test_job_args_and_collection_selection(cfg):
    assert parse_args([]).collections is None
    assert parse_args(["--collections", "products"]).collections == ["products"]
    assert set(select_collections(cfg, None)) == {"products", "customers"}
    assert select_collections(cfg, ["customers"]) == ["customers"]
    with pytest.raises(KeyError):
        select_collections(cfg, ["nope"])

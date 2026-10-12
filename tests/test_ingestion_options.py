"""Pure-Python tests for the ingestion option builders (no Spark needed).

These pin the Databricks gotchas called out in the plan so a refactor can't silently drop them.
"""
import pytest

from maven_market.config import load_config
from maven_market.ingestion.autoloader import (
    autoloader_options,
    bronze_table,
    checkpoint_path,
    source_path,
)
from maven_market.ingestion.kafka_stream import kafka_options, topic_settings
from maven_market.ingestion.mongo_ingest import mongo_options


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


# ---- Auto Loader (Review Focus #2: schema drift must not fail the stream) ----

def test_autoloader_has_required_options(cfg):
    o = autoloader_options(cfg, "customers")
    assert o["cloudFiles.format"] == "csv"
    assert o["cloudFiles.inferColumnTypes"] == "true"      # else every column is a string
    assert o["cloudFiles.schemaEvolutionMode"] == "rescue"  # drift -> rescued column, no failure
    assert o["rescuedDataColumn"] == "_rescued_data"       # NOTE: no 'cloudFiles.' prefix
    assert "rescuedDataColumn" in o and "cloudFiles.rescuedDataColumn" not in o
    assert o["header"] == "true"


def test_autoloader_schema_location_is_per_source_and_under_landing(cfg):
    a = autoloader_options(cfg, "customers")["cloudFiles.schemaLocation"]
    b = autoloader_options(cfg, "products")["cloudFiles.schemaLocation"]
    assert a != b
    assert a.startswith(cfg["storage"]["landing_root"])
    assert a.endswith("/customers")


def test_paths_and_tables_come_from_config(cfg):
    assert source_path(cfg, "transactions") == cfg["storage"]["landing_root"] + "/transactions"
    assert bronze_table(cfg, "transactions") == "maven_market_uc.bronze.transactions"
    assert checkpoint_path(cfg, "transactions").endswith("/transactions")


def test_unknown_source_raises(cfg):
    with pytest.raises(KeyError):
        source_path(cfg, "nope")


# ---- Kafka (Review Focus #1: shaded JAAS, SASL_SSL) ----

def test_kafka_options_use_shaded_jaas_and_sasl_ssl(cfg):
    o = kafka_options(cfg, "orders", "pkc-1:9092", "KEY", "SECRET")
    assert o["kafka.bootstrap.servers"] == "pkc-1:9092"
    assert o["subscribe"] == "orders"
    assert o["kafka.security.protocol"] == "SASL_SSL"
    assert o["kafka.sasl.mechanism"] == "PLAIN"
    jaas = o["kafka.sasl.jaas.config"]
    # unshaded class name triggers RESTRICTED_STREAMING_OPTION on Databricks
    assert jaas.startswith("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule")
    assert 'username="KEY"' in jaas and 'password="SECRET"' in jaas
    assert o["startingOffsets"] == "earliest"


def test_kafka_topic_settings_have_schema_and_required_keys(cfg):
    t = topic_settings(cfg, "orders")
    assert "order_id" in t["required"]
    assert "order_id STRING" in t["schema"]
    with pytest.raises(KeyError):
        topic_settings(cfg, "nope")


def test_kafka_secret_values_never_in_config(cfg):
    assert "SECRET" not in str(cfg) and cfg["kafka"]["secrets"]["api_secret"] == "confluent-api-secret"


# ---- MongoDB ----

def test_mongo_options(cfg):
    o = mongo_options(cfg, "mongodb+srv://u:p@c.mongodb.net/", "products")
    assert o["connection.uri"].startswith("mongodb+srv://")
    assert o["database"] == "maven_market"
    assert o["collection"] == "products"


def test_mongo_unknown_collection_raises(cfg):
    with pytest.raises(KeyError):
        mongo_options(cfg, "mongodb+srv://x", "nope")

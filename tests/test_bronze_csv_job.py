"""Pure-Python tests for the bronze CSV job entry point (Spark parts run on Databricks)."""
import pytest

from maven_market.config import load_config
from maven_market.jobs.bronze_csv import audit_table, parse_args, select_sources


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


def test_parse_args_defaults_to_dev_and_all_sources():
    a = parse_args([])
    assert a.env == "dev" and a.config_dir is None and a.sources is None


def test_parse_args_explicit():
    a = parse_args(["--env", "prod", "--config-dir", "/x/config", "--sources", "customers,stores"])
    assert (a.env, a.config_dir, a.sources) == ("prod", "/x/config", ["customers", "stores"])


def test_select_sources_defaults_to_every_csv_source(cfg):
    assert set(select_sources(cfg, None)) == set(cfg["sources"])


def test_select_sources_subset_preserves_request(cfg):
    assert select_sources(cfg, ["stores", "customers"]) == ["stores", "customers"]


def test_select_sources_unknown_raises(cfg):
    with pytest.raises(KeyError):
        select_sources(cfg, ["customers", "nope"])


def test_audit_table_is_fully_qualified_from_config(cfg):
    assert audit_table(cfg) == "maven_market_uc.audit.audit_logs"

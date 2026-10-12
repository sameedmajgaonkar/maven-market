"""Contract tests for the Silver/Gold SQL (pure Python): rendering, placeholders, config coverage."""
import re

import pytest

from maven_market.config import load_config
from maven_market.pipelines import transforms as T


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


def test_every_silver_and_gold_table_in_config_has_a_definition(cfg):
    # silver: SCD2 dims come from stream exprs, the rest from SQL
    silver_defined = set(T.SILVER_SQL) | set(T.SCD2_SOURCES)
    assert silver_defined == set(cfg["pipeline"]["silver"])
    assert set(T.GOLD_SQL) == set(cfg["pipeline"]["gold"])


@pytest.mark.parametrize("name", sorted(set(T.SILVER_SQL)))
def test_silver_sql_renders_fully(cfg, name):
    sql = T.render(T.SILVER_SQL[name], cfg)
    assert not re.search(r"[{}]", sql), f"unrendered placeholder in silver.{name}"
    assert "maven_market_uc.bronze." in sql


@pytest.mark.parametrize("name", sorted(set(T.GOLD_SQL)))
def test_gold_sql_renders_fully(cfg, name):
    sql = T.render(T.GOLD_SQL[name], cfg)
    assert not re.search(r"[{}]", sql), f"unrendered placeholder in gold.{name}"


def test_gold_reads_only_silver_or_gold_never_bronze(cfg):
    for name, template in T.GOLD_SQL.items():
        assert "bronze" not in T.render(template, cfg), f"gold.{name} must not read bronze directly"


def test_render_unknown_placeholder_raises(cfg):
    with pytest.raises(KeyError):
        T.render("SELECT * FROM {no_such_table}", cfg)


def test_bronze_names_are_resolved_from_config(cfg):
    n = T.names(cfg)
    assert n["bronze_customers"] == "maven_market_uc.bronze.customers"
    assert n["bronze_orders"] == "maven_market_uc.bronze.orders"
    assert n["silver_dim_customer"] == "maven_market_uc.silver.dim_customer"
    assert n["gold_fact_sales"] == "maven_market_uc.gold.fact_sales"


def test_data_region_expr_is_built_from_config_and_null_safe(cfg):
    expr = T.data_region_expr(cfg, "s.store_country")
    assert "try_element_at" in expr  # ANSI mode errors on a missing map key otherwise
    assert "'Canada'" in expr and "'NA'" in expr and "'UNKNOWN'" in expr


def test_data_region_expr_rejects_quotes_in_config(cfg):
    bad = {**cfg, "pipeline": {**cfg["pipeline"], "data_region_by_country": {"U'SA": "NA"}}}
    with pytest.raises(ValueError):
        T.data_region_expr(bad, "x")


def test_scd2_sources_have_keys_and_sequence_column_in_exprs():
    for name, spec in T.SCD2_SOURCES.items():
        assert spec["keys"] and spec["sequence_by"] == "_source_file_modified_at"
        joined = " ".join(spec["exprs"])
        for key in spec["keys"]:
            assert key in joined
        assert "_source_file_modified_at" in joined


def test_output_columns_handles_aliases_and_bare_columns():
    exprs = ["customer_id", "trim(first_name) AS first_name", "(x = 'Y') as is_homeowner", "_ingested_at"]
    assert T.output_columns(exprs) == ["customer_id", "first_name", "is_homeowner", "_ingested_at"]


def test_scd2_history_ignore_list_only_names_real_output_columns(cfg):
    for name, spec in T.SCD2_SOURCES.items():
        ignore = T.scd2_ignore_columns(cfg, name)
        assert set(ignore) <= set(T.output_columns(spec["exprs"]))
        assert "_source_file_modified_at" in ignore  # the sequence column must never create versions
        assert "_rescued_data" not in ignore        # not selected, so must not be listed
        assert not set(ignore) & set(spec["keys"])


def test_scd2_ignore_columns_include_lineage_and_sequence(cfg):
    ignore = set(cfg["pipeline"]["scd2_ignore_columns"])
    assert {"_source_file_modified_at", "_ingested_at", "_rescued_data"} <= ignore


def test_expectation_columns_exist_in_silver_outputs():
    """Rules for the SCD2 dims reference columns the view must actually produce."""
    from maven_market.config import load_expectations

    exp = load_expectations()
    cust = " ".join(T.SCD2_SOURCES["dim_customer"]["exprs"])
    for needed in ("birthdate", "income_low_k", "gender"):
        assert needed in cust
    assert "customer_id" in cust
    prod = " ".join(T.SCD2_SOURCES["dim_product"]["exprs"])
    for needed in ("product_retail_price", "product_cost"):
        assert needed in prod
    assert exp["dim_customer"]["drop"]

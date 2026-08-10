# Databricks notebook source
# mypy: ignore-errors
# pyright: reportMissingImports=false, reportUndefinedVariable=false
# ============================================================
# Declarative Pipeline: structured_yaml/declarative/03_transform_to_silver
# lib/structured_yaml/silver.py のロジックを @dp.materialized_view でラップ。
# 不良データ検出は @dp.expect 系で宣言的に行う。他テーブル参照は spark.read.table。
# ============================================================

# --- import path 準備 ---
import os
import sys

_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "lib")):
    _root = os.path.dirname(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

from pyspark import pipelines as dp

from lib.structured_yaml.config import parse_yaml_config
from lib.structured_yaml.silver import transform_company_a, transform_company_b

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"


def load_config(client_id: str) -> dict:
    with open(f"{VOLUME_PATH}/configs/{client_id}.yaml") as f:
        return parse_yaml_config(f.read())


@dp.materialized_view(name="silver_company_a", comment="A社 CDM (rename only)")
@dp.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
@dp.expect("has_base_salary", "base_salary IS NOT NULL")
def silver_company_a():
    df = spark.read.table("bronze_company_a").where("_corrupt IS NULL")  # noqa: F821
    return transform_company_a(df, load_config("company_a"))


@dp.materialized_view(name="silver_company_b", comment="B社 CDM (clean & normalize)")
@dp.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
@dp.expect("has_base_salary", "base_salary IS NOT NULL")
@dp.expect("has_join_date", "join_date IS NOT NULL")
def silver_company_b():
    df = spark.read.table("bronze_company_b").where("_corrupt IS NULL")  # noqa: F821
    return transform_company_b(df, load_config("company_b"))


@dp.materialized_view(name="silver_cdm", comment="全クライアント統合CDM")
def silver_cdm():
    a = spark.read.table("silver_company_a")  # noqa: F821
    b = spark.read.table("silver_company_b")  # noqa: F821
    return a.unionByName(b)

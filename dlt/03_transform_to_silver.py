# Databricks notebook source
# ============================================================
# Pipeline (Declarative): dlt/03_transform_to_silver  (オーケストレーション層)
# 目的: lib の変換ロジックを @dlt.table でラップし、シルバー層(CDM)を宣言する。
#       A社/B社の変換の実体は lib/silver.py (共通File) にあり、pyspark版と共有。
#       不良データ検出は @dlt.expect 系で宣言的に行う。
#
# ※ dlt/02_ingest_to_bronze.py と同じパイプラインに含めて実行する。
#   silver_* が bronze_* に依存することをフレームワークが自動解析する。
# ============================================================

# --- import path 準備: 共通 lib/ を import できるよう repo root を sys.path に追加 ---
import os
import sys

_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "lib")):
    _root = os.path.dirname(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

import dlt

from lib.config import parse_yaml_config
from lib.silver import transform_company_a, transform_company_b

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"


def load_config(client_id: str) -> dict:
    with open(f"{VOLUME_PATH}/configs/{client_id}.yaml") as f:
        return parse_yaml_config(f.read())


# ============================================================
# A社 シルバー: リネームのみ (ロジックは lib/silver.transform_company_a)
# ============================================================
@dlt.table(
    name="silver_company_a",
    comment="A社 CDM (rename only)",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
@dlt.expect("has_base_salary", "base_salary IS NOT NULL")
def silver_company_a():
    df = dlt.read("bronze_company_a").where("_corrupt IS NULL")
    return transform_company_a(df, load_config("company_a"))


# ============================================================
# B社 シルバー: クレンジング & 正規化 (ロジックは lib/silver.transform_company_b)
# ============================================================
@dlt.table(
    name="silver_company_b",
    comment="B社 CDM (clean & normalize)",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
@dlt.expect("has_base_salary", "base_salary IS NOT NULL")  # "非公開"→null を警告集計
@dlt.expect("has_join_date", "join_date IS NOT NULL")  # invalid_date→null を警告集計
def silver_company_b():
    df = dlt.read("bronze_company_b").where("_corrupt IS NULL")
    return transform_company_b(df, load_config("company_b"))


# ============================================================
# CDM統合: 各社シルバーを union (依存はフレームワークが自動解決)
# ============================================================
@dlt.table(
    name="silver_cdm",
    comment="全クライアント統合の共通データモデル (CDM)",
    table_properties={"quality": "silver"},
)
def silver_cdm():
    return dlt.read("silver_company_a").unionByName(dlt.read("silver_company_b"))

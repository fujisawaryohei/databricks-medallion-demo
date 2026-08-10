# Databricks notebook source
# mypy: ignore-errors
# pyright: reportMissingImports=false, reportUndefinedVariable=false
# ============================================================
# Declarative Pipeline: structured_yaml/declarative/02_ingest_to_bronze
# Spark Declarative Pipelines (pyspark.pipelines / 旧DLT) で Bronze を宣言。
# CSVはバッチ読み込みのため materialized_view。ロジックは lib/structured_yaml/。
# ============================================================

# --- import path 準備: 共通 lib/ を import できるよう repo root を sys.path に追加 ---
import os
import sys

_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "lib")):
    _root = os.path.dirname(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

from pyspark import pipelines as dp

from lib.structured_yaml.bronze import add_ingest_metadata, read_source
from lib.structured_yaml.config import build_schema_from_yaml, parse_yaml_config

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"


def load_config(client_id: str) -> dict:
    with open(f"{VOLUME_PATH}/configs/{client_id}.yaml") as f:
        return parse_yaml_config(f.read())


def define_bronze_view(client_id: str):
    """client_id ごとに Bronze マテリアライズドビューを1つ宣言する(配線)。"""
    config = load_config(client_id)
    schema = build_schema_from_yaml(config)

    @dp.materialized_view(
        name=f"bronze_{client_id}",
        comment=f"{config['client_name']} raw ingestion (PERMISSIVE mode)",
    )
    def _bronze():
        df = read_source(spark, config["source"], schema)  # noqa: F821 (spark自動注入)
        return add_ingest_metadata(df, client_id)

    return _bronze


define_bronze_view("company_a")
define_bronze_view("company_b")

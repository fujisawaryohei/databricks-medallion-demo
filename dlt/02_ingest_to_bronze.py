# Databricks notebook source
# ============================================================
# Pipeline (Declarative): dlt/02_ingest_to_bronze  (オーケストレーション層)
# 目的: lib の取り込みロジックを @dlt.table でラップし、ブロンズ層を宣言する。
#       スキーマ生成・読み込みの実体は lib/ (共通File) にあり、pyspark版と共有。
#
# ※ Lakeflow Declarative Pipeline のソースとして登録して実行する。
#   spark はパイプライン実行時に自動注入される。YAMLは /Volumes を open() で読む。
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

from lib.bronze import add_ingest_metadata, read_source
from lib.config import build_schema_from_yaml, parse_yaml_config

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"


def load_config(client_id: str) -> dict:
    """Pipeline実行クラスタ上では /Volumes をFUSE経由で open() できる。"""
    with open(f"{VOLUME_PATH}/configs/{client_id}.yaml") as f:
        return parse_yaml_config(f.read())


def define_bronze_table(client_id: str):
    """client_id ごとに @dlt.table を1つ宣言する (配線)。ロジックは lib。"""
    config = load_config(client_id)
    schema = build_schema_from_yaml(config)

    @dlt.table(
        name=f"bronze_{client_id}",
        comment=f"{config['client_name']} raw ingestion (PERMISSIVE mode)",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        df = read_source(spark, config["source"], schema)  # noqa: F821 (spark自動注入)
        return add_ingest_metadata(df, client_id)

    return _bronze


# --- 各社のブロンズテーブルを宣言 ---
define_bronze_table("company_a")
define_bronze_table("company_b")

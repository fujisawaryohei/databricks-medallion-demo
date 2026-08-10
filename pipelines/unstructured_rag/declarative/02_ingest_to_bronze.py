# Databricks notebook source
# mypy: ignore-errors
# pyright: reportMissingImports=false, reportUndefinedVariable=false
# ============================================================
# Declarative Pipeline: unstructured_rag/declarative/02_ingest_to_bronze
# 生バイナリを Bronze に取り込む。config の source.mode で切替:
#   batch      … UC Volumes 一括読み → @dp.materialized_view
#   autoloader … Auto Loader 増分     → @dp.table(ストリーミング)
# ロジックは lib/unstructured_rag/bronze.py。
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

from lib.unstructured_rag.bronze import read_binary, to_bronze
from lib.unstructured_rag.config import parse_yaml_config

VOLUME_PATH = "/Volumes/demo_catalog/rag/raw_docs"


def load_parsing_config() -> dict:
    with open(f"{VOLUME_PATH}/configs/parsing.yaml") as f:
        return parse_yaml_config(f.read())


_config = load_parsing_config()
_source = _config["source"]
_mode = _source.get("mode", "batch")  # batch | autoloader

_COMMENT = "PDF/PPTX/MD raw binary ingestion"


def _bronze_df():
    return to_bronze(
        read_binary(spark, _source["path"], _mode, _source.get("path_glob_filter"))  # noqa: F821
    )


# 取り込みモードに応じてテーブル種別(デコレータ)を切り替える。
# batch=マテリアライズドビュー / autoloader=ストリーミングテーブル。
if _mode == "autoloader":

    @dp.table(name="bronze_docs", comment=f"{_COMMENT} (Auto Loader)")
    def bronze_docs():
        return _bronze_df()

else:

    @dp.materialized_view(name="bronze_docs", comment=f"{_COMMENT} (batch / UC Volumes)")
    def bronze_docs():
        return _bronze_df()

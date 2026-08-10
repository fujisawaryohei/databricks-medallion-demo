# Databricks notebook source
# mypy: ignore-errors
# pyright: reportMissingImports=false, reportUndefinedVariable=false
# ============================================================
# Declarative Pipeline: unstructured_rag/declarative/04_chunk_to_gold
# Silver の長文をチャンク分割し、権限メタを付与して Gold を宣言。
# バッチ処理のため @dp.materialized_view。silver_docs への依存は自動解決。
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

from lib.unstructured_rag.config import parse_yaml_config
from lib.unstructured_rag.gold import to_gold_chunks

VOLUME_PATH = "/Volumes/demo_catalog/rag/raw_docs"


def load_parsing_config() -> dict:
    with open(f"{VOLUME_PATH}/configs/parsing.yaml") as f:
        return parse_yaml_config(f.read())


@dp.materialized_view(
    name="gold_chunks",
    comment="Chunked text with access-control metadata",
)
def gold_chunks():
    config = load_parsing_config()
    return to_gold_chunks(spark.read.table("silver_docs"), config)  # noqa: F821

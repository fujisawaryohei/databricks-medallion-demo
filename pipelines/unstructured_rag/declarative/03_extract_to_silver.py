# Databricks notebook source
# mypy: ignore-errors
# pyright: reportMissingImports=false, reportUndefinedVariable=false
# ============================================================
# Declarative Pipeline: unstructured_rag/declarative/03_extract_to_silver
# 生バイナリ → テキスト抽出・クレンジング・PII・重複排除 → Silver。
# バッチ処理のため @dp.materialized_view。抽出失敗は @dp.expect_or_drop で除外。
# ============================================================

# --- import path 準備 ---
import os
import sys

_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "lib")):
    _root = os.path.dirname(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

from pyspark import pipelines as dp  # type: ignore
from pyspark.sql.functions import col, create_map, lit, udf
from pyspark.sql.types import StringType

from lib.unstructured_rag.silver import (
    add_extracted_text,
    dedup_documents,
    make_document_id,
)


@dp.materialized_view(
    name="silver_docs",
    comment="Extracted & cleaned document-level text",
)
@dp.expect_or_drop("has_extracted_text", "extracted_text IS NOT NULL")
def silver_docs():
    doc_id_udf = udf(make_document_id, StringType())

    df = spark.read.table("bronze_docs")  # noqa: F821 (spark自動注入)
    df = df.withColumn("document_id", doc_id_udf(col("file_path")))
    df = add_extracted_text(df)
    df = dedup_documents(df)

    return df.select(
        "document_id",
        "extracted_text",
        "source_type",
        create_map(
            lit("file_path"),
            col("file_path"),
            lit("ingest_timestamp"),
            col("ingest_timestamp").cast("string"),
        ).alias("metadata"),
    )

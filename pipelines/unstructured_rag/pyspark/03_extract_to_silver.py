# Databricks notebook source
# ============================================================
# Notebook: unstructured_rag/pyspark/03_extract_to_silver  (命令的 / 配線)
# 生バイナリ → テキスト抽出・クレンジング・PII・重複排除 → Silver。
# ロジックは lib/unstructured_rag/silver.py。
# ============================================================

# COMMAND ----------

# --- import path 準備 ---
import os
import sys

_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "lib")):
    _root = os.path.dirname(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

# COMMAND ----------

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import col, create_map, lit, udf
from pyspark.sql.types import StringType

from lib.unstructured_rag.silver import (
    add_extracted_text,
    dedup_documents,
    make_document_id,
)

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

BRONZE_TABLE = "demo_catalog.rag.bronze_docs"
SILVER_TABLE = "demo_catalog.rag.silver_docs"

# COMMAND ----------

doc_id_udf = udf(make_document_id, StringType())

df = spark.table(BRONZE_TABLE)
df = df.withColumn("document_id", doc_id_udf(col("file_path")))

# テキスト抽出(source_type判定 → 抽出 → クレンジング → PIIマスク)
df = add_extracted_text(df)

# 抽出成功のみ残し、ほぼ同一ドキュメントを重複排除
df = df.where(col("extracted_text").isNotNull())
df = dedup_documents(df)

# COMMAND ----------

# Silver スキーマに整形(metadataはmap型)
silver = df.select(
    "document_id",
    "extracted_text",
    "source_type",
    create_map(
        lit("file_path"), col("file_path"),
        lit("ingest_timestamp"), col("ingest_timestamp").cast("string"),
    ).alias("metadata"),
)

silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(SILVER_TABLE)

print(f"✅ Silver 書き込み完了: {silver.count()}件 → {SILVER_TABLE}")
silver.select("document_id", "source_type").show(truncate=False)

# COMMAND ----------

# 抽出テキストの中身確認(PIIがマスクされているか)
silver.select("extracted_text").show(truncate=False)

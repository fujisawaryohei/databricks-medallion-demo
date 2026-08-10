# Databricks notebook source
# ============================================================
# Notebook: unstructured_rag/pyspark/04_chunk_to_gold  (命令的 / 配線)
# Silver の長文テキストをチャンク分割し、権限メタを付与して Gold へ。
# ロジックは lib/unstructured_rag/gold.py。
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

from lib.unstructured_rag.config import parse_yaml_config
from lib.unstructured_rag.gold import to_gold_chunks

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

VOLUME_PATH = "/Volumes/demo_catalog/rag/raw_docs"
SILVER_TABLE = "demo_catalog.rag.silver_docs"
GOLD_TABLE = "demo_catalog.rag.gold_chunks"


def load_parsing_config() -> dict:
    content = dbutils.fs.head(f"{VOLUME_PATH}/configs/parsing.yaml", 100000)
    return parse_yaml_config(content)


# COMMAND ----------

config = load_parsing_config()
silver = spark.table(SILVER_TABLE)

# チャンク分割(md=見出し保持 / pdf・pptx=段落ベース) + 権限メタ付与
gold = to_gold_chunks(silver, config)

gold.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(GOLD_TABLE)

print(f"✅ Gold 書き込み完了: {gold.count()}チャンク → {GOLD_TABLE}")

# COMMAND ----------

gold.select("chunk_id", "document_id", "chunk_index", "allowed_roles", "department").show(
    truncate=False
)

# COMMAND ----------

# チャンク本文の確認
gold.select("chunk_index", "chunk_text").orderBy("document_id", "chunk_index").show(
    truncate=80
)

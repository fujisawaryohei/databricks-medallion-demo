# Databricks notebook source
# ============================================================
# Notebook: unstructured_rag/pyspark/02_ingest_to_bronze  (命令的 / 配線)
# PDF/PPTX/MD の生バイナリを Bronze に取り込む。
# 取り込みモードは config の source.mode で切替:
#   batch      … UC Volumes 一括読み(動作検証向け・今回の既定)
#   autoloader … Auto Loader 増分ストリーミング(本番向け)
# ロジックは lib/unstructured_rag/bronze.py。
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

from lib.unstructured_rag.bronze import read_binary, to_bronze
from lib.unstructured_rag.config import parse_yaml_config

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

VOLUME_PATH = "/Volumes/demo_catalog/rag/raw_docs"
BRONZE_TABLE = "demo_catalog.rag.bronze_docs"


def load_parsing_config() -> dict:
    content = dbutils.fs.head(f"{VOLUME_PATH}/configs/parsing.yaml", 100000)
    return parse_yaml_config(content)


# COMMAND ----------

config = load_parsing_config()
source = config["source"]
mode = source.get("mode", "batch")  # batch | autoloader

df = to_bronze(read_binary(spark, source["path"], mode, source.get("path_glob_filter")))

if mode == "autoloader":
    # 増分ストリーミング: 今ある未処理分を処理して停止(availableNow)
    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", f"{VOLUME_PATH}/_checkpoints/bronze_docs")
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()
else:
    # バッチ: Volumes のファイルを一括で上書き取り込み
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(BRONZE_TABLE)

print(f"✅ Bronze 取り込み完了 (mode={mode}) → {BRONZE_TABLE}")

# COMMAND ----------

spark.table(BRONZE_TABLE).select("file_path", "ingest_timestamp").show(truncate=False)

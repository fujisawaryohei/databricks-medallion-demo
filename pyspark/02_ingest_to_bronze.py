# Databricks notebook source
# ============================================================
# Notebook: pyspark/02_ingest_to_bronze  (命令的版 / オーケストレーション層)
# 目的: lib のロジックを組み合わせ、各社データをブロンズ層へ取り込む。
#       変換・スキーマ生成の実体は lib/ (共通File) にあり、ここは「配線」に徹する。
# ============================================================

# COMMAND ----------

# --- import path 準備: 共通 lib/ を import できるよう repo root を sys.path に追加 ---
# このNotebookは pyspark/ 配下にあるため、lib/ のある階層を探して追加する。
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

from lib.bronze import add_ingest_metadata, read_source
from lib.config import build_schema_from_yaml, parse_yaml_config

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"

# COMMAND ----------


def load_config(client_id: str) -> dict:
    """ローカル(Databricks Connect)ではVolumeをdbutils経由で読む。"""
    content = dbutils.fs.head(f"{VOLUME_PATH}/configs/{client_id}.yaml", 100000)
    return parse_yaml_config(content)


def ingest_to_bronze(client_id: str):
    """設定を読み込み、lib のロジックでブロンズ層Delta Tableに書き込む配線。"""
    config = load_config(client_id)
    schema = build_schema_from_yaml(config)

    df = read_source(spark, config["source"], schema)
    df = add_ingest_metadata(df, client_id)

    table_name = f"demo_catalog.data_engineering.bronze_{client_id}"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    total = df.count()
    corrupt = df.where("_corrupt IS NOT NULL").count()
    print(f"✅ {config['client_name']}: {total}行 (正常{total - corrupt}/不良{corrupt}) → {table_name}")
    return df


# COMMAND ----------

# --- 実行: A社 ---
df_a = ingest_to_bronze("company_a")

# COMMAND ----------

# --- 実行: B社 ---
df_b = ingest_to_bronze("company_b")

# COMMAND ----------

# --- 不良データ確認 (B社) ---
print("--- B社 不良データ (_corrupt IS NOT NULL) ---")
df_b.where("_corrupt IS NOT NULL").show(truncate=False)

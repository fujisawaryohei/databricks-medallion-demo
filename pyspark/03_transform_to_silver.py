# Databricks notebook source
# ============================================================
# Notebook: pyspark/03_transform_to_silver  (命令的版 / オーケストレーション層)
# 目的: lib の変換ロジックを組み合わせ、ブロンズ層→シルバー層(CDM)へ統合する。
#       A社/B社の変換の実体は lib/silver.py (共通File) にあり、ここは「配線」に徹する。
# ============================================================

# COMMAND ----------

# --- import path 準備: 共通 lib/ を import できるよう repo root を sys.path に追加 ---
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

from lib.config import parse_yaml_config
from lib.silver import transform_company_a, transform_company_b

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"
SILVER_TABLE = "demo_catalog.data_engineering.silver_cdm"


def load_config(client_id: str) -> dict:
    content = dbutils.fs.head(f"{VOLUME_PATH}/configs/{client_id}.yaml", 100000)
    return parse_yaml_config(content)


def read_clean_bronze(client_id: str):
    """ブロンズ層を読み、PERMISSIVEで隔離した不良行を除外する。"""
    return spark.table(
        f"demo_catalog.data_engineering.bronze_{client_id}"
    ).where("_corrupt IS NULL")


# COMMAND ----------

# --- A社/B社をそれぞれ変換 (ロジックは lib/silver.py) ---
df_a = transform_company_a(read_clean_bronze("company_a"), load_config("company_a"))
df_b = transform_company_b(read_clean_bronze("company_b"), load_config("company_b"))

# COMMAND ----------

# --- CDM統合 & シルバー層書き込み ---
df_cdm = df_a.unionByName(df_b)
df_cdm.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

print(f"✅ シルバー層CDM 書き込み完了: {df_cdm.count()}行 → {SILVER_TABLE}")
df_cdm.show(truncate=False)

# COMMAND ----------

# --- 品質確認: クライアント×部門別 レコード数 ---
df_cdm.groupBy("source_client", "department").count().orderBy(
    "source_client", "department"
).show()

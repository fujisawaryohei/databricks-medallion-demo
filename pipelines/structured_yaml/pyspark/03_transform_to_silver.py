# Databricks notebook source
# ============================================================
# Notebook: structured_yaml/pyspark/03_transform_to_silver  (命令的 / 配線)
# ロジックは lib/structured_yaml/silver.py にあり、ここは組み合わせに徹する。
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

from lib.structured_yaml.config import parse_yaml_config
from lib.structured_yaml.silver import transform_company_a, transform_company_b

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"
SILVER_TABLE = "demo_catalog.data_engineering.silver_cdm"


def load_config(client_id: str) -> dict:
    content = dbutils.fs.head(f"{VOLUME_PATH}/configs/{client_id}.yaml", 100000)
    return parse_yaml_config(content)


def read_clean_bronze(client_id: str):
    return spark.table(
        f"demo_catalog.data_engineering.bronze_{client_id}"
    ).where("_corrupt IS NULL")


# COMMAND ----------

df_a = transform_company_a(read_clean_bronze("company_a"), load_config("company_a"))
df_b = transform_company_b(read_clean_bronze("company_b"), load_config("company_b"))

# COMMAND ----------

df_cdm = df_a.unionByName(df_b)
df_cdm.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

print(f"✅ シルバー層CDM 書き込み完了: {df_cdm.count()}行 → {SILVER_TABLE}")
df_cdm.show(truncate=False)

# COMMAND ----------

df_cdm.groupBy("source_client", "department").count().orderBy(
    "source_client", "department"
).show()

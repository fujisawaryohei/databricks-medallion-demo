# Databricks notebook source
# ============================================================
# Notebook: unstructured_rag/setup/01_create_parsing_configs
# 目的: パーサー/チャンキング/権限メタの設定YAMLを Volume に配置する。
# ============================================================

# COMMAND ----------

# --- ローカル実行用セットアップ (Databricks上では不要) ---
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

VOLUME_PATH = "/Volumes/demo_catalog/rag/raw_docs"

# COMMAND ----------

# --- パーサー/チャンキング設定 ---
#   chunking: source_type ごとの分割戦略
#     md   → 見出し構造を保持する markdown_header
#     pdf/pptx → 段落ベース(paragraph)
#   permission_metadata: 各チャンクに継承させるアクセス制御属性
parsing_config_yaml = """
source:
  path: "/Volumes/demo_catalog/rag/raw_docs/docs/"
  mode: batch              # batch(UC Volumes一括・検証向け) | autoloader(増分・本番向け)
  # path_glob_filter: "*.md"   # 省略可(未対応拡張子はSilver抽出時に自動除外)

chunking:
  md:
    strategy: markdown_header
  pdf:
    strategy: paragraph
    max_chars: 1000
    overlap: 100
  pptx:
    strategy: paragraph
    max_chars: 800
    overlap: 80
  default:
    strategy: paragraph
    max_chars: 1000
    overlap: 100

permission_metadata:
  allowed_roles:
    - analyst
    - manager
  department: engineering
"""

dbutils.fs.put(f"{VOLUME_PATH}/configs/parsing.yaml", parsing_config_yaml, overwrite=True)
print("✅ パーサー/チャンキング設定を作成しました")

# COMMAND ----------

# --- 確認 ---
for f in dbutils.fs.ls(f"{VOLUME_PATH}/configs/"):
    print(f.name, f.size)

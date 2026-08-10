# Databricks notebook source
# ============================================================
# Notebook: unstructured_rag/setup/00_setup_sample_docs
# 目的: RAG用の Volume を作成し、サンプルの Markdown ドキュメントを配置する。
#       PDF/PPTX は同じ Volume に手動アップロードすれば同パイプラインで処理される。
# ============================================================

# COMMAND ----------

# --- ローカル実行用セットアップ (Databricks上では不要) ---
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

# COMMAND ----------

# --- Unity Catalog: RAG用スキーマ/Volume の作成 ---
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("USE CATALOG demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS rag")
spark.sql("USE SCHEMA rag")
spark.sql("CREATE VOLUME IF NOT EXISTS raw_docs")

VOLUME_PATH = "/Volumes/demo_catalog/rag/raw_docs"

# COMMAND ----------

# --- サンプル Markdown (見出し構造あり / PII(メール)を含みredactionを検証) ---
engineering_md = """# エンジニアリングガイド

## 概要
本ドキュメントは開発チーム向けの内部ガイドです。

## 連絡先
不明点は support@example.com または 03-1234-5678 まで連絡してください。

## デプロイ手順
1. main ブランチにマージする
2. CI が通ったことを確認する
3. 本番へデプロイする
"""

sales_md = """# 営業ポリシー

## 行動指針
顧客第一で対応すること。

## 見積もりルール
値引きは20%までとする。例外は上長承認が必要。
"""

# COMMAND ----------

# --- Volumes に書き込み ---
dbutils.fs.put(f"{VOLUME_PATH}/docs/engineering_guide.md", engineering_md, overwrite=True)
dbutils.fs.put(f"{VOLUME_PATH}/docs/sales_policy.md", sales_md, overwrite=True)

print("✅ サンプル Markdown を配置しました")
print("   PDF/PPTX を試す場合は、実ファイルを次のパスへアップロードしてください:")
print(f"   {VOLUME_PATH}/docs/")

# COMMAND ----------

# --- 確認 ---
for f in dbutils.fs.ls(f"{VOLUME_PATH}/docs/"):
    print(f.name, f.size)

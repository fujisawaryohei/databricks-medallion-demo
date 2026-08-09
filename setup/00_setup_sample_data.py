# Databricks notebook source
# ============================================================
# Notebook: setup/00_setup_sample_data
# 目的: Unity Catalog に Volume を作成し、サンプルCSVを配置する。
#       pyspark版・dlt版どちらのパイプラインでも共通の前提セットアップ。
# ============================================================

# COMMAND ----------

# --- ローカル実行用セットアップ (Databricks上では不要) ---
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

# COMMAND ----------

# --- Unity Catalog Volume の作成 ---
spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("USE CATALOG demo_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS data_engineering")
spark.sql("USE SCHEMA data_engineering")
spark.sql("CREATE VOLUME IF NOT EXISTS raw_files")

# COMMAND ----------

# --- A社サンプルデータ (英語カラム名、数値型が正しい) ---
company_a_csv = """Emp_No,BasePay,Department,JoinDate
A001,350000,Sales,2020-04-01
A002,420000,Engineering,2019-07-15
A003,380000,HR,2021-01-10
A004,510000,Engineering,2018-03-20
A005,290000,Sales,2022-06-01
"""

# --- B社サンプルデータ (日本語カラム名、基本給が文字列、一部不良データあり) ---
company_b_csv = """社員番号,基本給,部門,入社日
B001,¥300000,営業部,2020/04/01
B002,450000,開発部,2019/07/15
B003,非公開,人事部,2021/01/10
B004,380000,開発部,invalid_date
B005,320000,営業部,2022/06/01
"""

# COMMAND ----------

# --- Volumesに書き込み ---
volume_path = "/Volumes/demo_catalog/data_engineering/raw_files"

dbutils.fs.put(f"{volume_path}/company_a/employees.csv", company_a_csv, overwrite=True)
dbutils.fs.put(f"{volume_path}/company_b/employees.csv", company_b_csv, overwrite=True)

print("✅ サンプルデータを Volumes に配置しました")

# COMMAND ----------

# --- 確認 ---
for f in dbutils.fs.ls(f"{volume_path}/company_a/"):
    print(f.name, f.size)

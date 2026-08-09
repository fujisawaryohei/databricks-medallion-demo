from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

# spark セッション（処理はクラスタ上で実行される）
spark = DatabricksSession.builder.getOrCreate()

# dbutils（fs, secrets などが使える）
dbutils = WorkspaceClient().dbutils

# Databricks notebook source
# ============================================================
# Notebook: 03_transform_to_silver
# 目的: ブロンズ層からYAML設定のカラムマッピング＆変換ルールを
#       適用し、共通データモデル(CDM)としてシルバー層に統合する
# ============================================================

# COMMAND ----------

import yaml
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    regexp_replace,
    to_date,
    when,
)
from pyspark.sql.types import IntegerType

# COMMAND ----------


# YAML読み込み (02_ingest_to_bronzeと同じユーティリティ)
def load_yaml_config(path: str) -> dict:
    content = dbutils.fs.head(path, 100000)
    return yaml.safe_load(content)


volume_path = "/Volumes/demo_catalog/data_engineering/raw_files"
config_a = load_yaml_config(f"{volume_path}/configs/company_a.yaml")
config_b = load_yaml_config(f"{volume_path}/configs/company_b.yaml")

# COMMAND ----------


# --- A社: ブロンズ → シルバー変換 ---
def transform_company_a(config: dict):
    """A社: カラムリネームのみでCDMへマッピング"""
    mapping = config["column_mapping"]
    permissions = config["permission_metadata"]

    df = spark.table("demo_catalog.data_engineering.bronze_company_a")

    # 正常データのみ抽出
    df = df.where("_corrupt IS NULL")

    # カラムマッピング適用
    for src_col, dst_col in mapping.items():
        df = df.withColumnRenamed(src_col, dst_col)

    # 権限メタデータ付与
    df = (
        df.withColumn("allowed_roles", lit(",".join(permissions["allowed_roles"])))
        .withColumn("source_client", lit(config["client_id"]))
        .withColumn("_transformed_at", current_timestamp())
        .select(
            "employee_id",
            "base_salary",
            "department",
            "join_date",
            "allowed_roles",
            "source_client",
            "_transformed_at",
        )
    )
    return df


# COMMAND ----------


# --- B社: ブロンズ → シルバー変換 (複雑な変換あり) ---
def transform_company_b(config: dict):
    """B社: 文字列→数値変換、日本語→英語マッピング、日付変換"""
    mapping = config["column_mapping"]
    transforms = config["transformations"]
    permissions = config["permission_metadata"]
    dept_map = transforms["department"]["mapping"]

    df = spark.table("demo_catalog.data_engineering.bronze_company_b")

    # 正常データのみ抽出
    df = df.where("_corrupt IS NULL")

    # カラムリネーム
    for src_col, dst_col in mapping.items():
        df = df.withColumnRenamed(src_col, dst_col)

    # base_salary: ¥記号除去 → 数値変換 (変換失敗はnull)
    df = df.withColumn(
        "base_salary",
        regexp_replace(col("base_salary"), "[¥,]", "").cast(IntegerType()),
    )

    # department: 日本語 → 英語マッピング
    dept_expr = col("department")
    for jp, en in dept_map.items():
        dept_expr = when(col("department") == jp, lit(en)).otherwise(dept_expr)
    df = df.withColumn("department", dept_expr)

    # join_date: yyyy/MM/dd → date型
    df = df.withColumn("join_date", to_date(col("join_date"), "yyyy/MM/dd"))

    # 権限メタデータ付与
    df = (
        df.withColumn("allowed_roles", lit(",".join(permissions["allowed_roles"])))
        .withColumn("source_client", lit(config["client_id"]))
        .withColumn("_transformed_at", current_timestamp())
        .select(
            "employee_id",
            "base_salary",
            "department",
            "join_date",
            "allowed_roles",
            "source_client",
            "_transformed_at",
        )
    )
    return df


# COMMAND ----------

# ============================================================
# 実行: 各社を変換
# ============================================================
df_silver_a = transform_company_a(config_a)
print("--- A社 シルバー変換結果 ---")
display(df_silver_a)

# COMMAND ----------

df_silver_b = transform_company_b(config_b)
print("--- B社 シルバー変換結果 ---")
display(df_silver_b)

# COMMAND ----------

# ============================================================
# CDM統合 & シルバー層書き込み
# ============================================================
df_cdm = df_silver_a.unionByName(df_silver_b)

df_cdm.write.format("delta").mode("overwrite").saveAsTable(
    "demo_catalog.data_engineering.silver_cdm"
)

print("✅ シルバー層 CDM 書き込み完了!")
print(f"   統合レコード数: {df_cdm.count()}")
display(df_cdm)

# COMMAND ----------

# ============================================================
# データ品質チェック
# ============================================================
print("--- クライアント×部門別 レコード数 ---")
display(
    df_cdm.groupBy("source_client", "department")
    .count()
    .orderBy("source_client", "department")
)

# COMMAND ----------

# --- NULL値レポート ---
from pyspark.sql.functions import sum as spark_sum

print("--- NULL値レポート (各列のNULL件数) ---")
display(
    df_cdm.select(
        [
            spark_sum(col(c).isNull().cast("int")).alias(c)
            for c in ["employee_id", "base_salary", "department", "join_date"]
        ]
    )
)

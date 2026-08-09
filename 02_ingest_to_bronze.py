# Databricks notebook source
# ============================================================
# Notebook: 02_ingest_to_bronze
# 目的: YAML設定を読み込み、動的にStructTypeを生成し、
#       PERMISSIVEモードでブロンズ層に取り込む
# ============================================================

# COMMAND ----------

# %pip install pyyaml
# ↑ クラスタにyamlが入っていない場合のみコメントアウトを外してください

# COMMAND ----------

import yaml
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType

dbutils = WorkspaceClient.dbutils
spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------


# --- YAML読み込みユーティリティ ---
def load_yaml_config(path: str) -> dict:
    """Volumes上のYAMLファイルを読み込んでdictとして返す"""
    content = dbutils.fs.head(path, 100000)
    return yaml.safe_load(content)


# COMMAND ----------


# --- StructType動的生成 ---
def build_schema_from_yaml(config: dict) -> StructType:
    """YAML設定のschemaセクションからSpark StructTypeを動的に生成し、
    _corrupt列を追加する (PERMISSIVEモード用)"""
    schema_json = config["schema"]
    base_schema = StructType.fromJson(schema_json)

    # 不良データ隔離用カラムを動的に追加
    base_schema = base_schema.add(
        StructField(
            "_corrupt",
            StringType(),
            True,
            {"comment": "invalid rows captured by PERMISSIVE mode"},
        )
    )
    return base_schema


# COMMAND ----------


# --- ブロンズ層取り込み関数 ---
def ingest_to_bronze(config: dict):
    """設定に基づいてデータを読み込み、ブロンズ層Delta Tableに書き込む"""
    client_id = config["client_id"]
    source = config["source"]
    schema = build_schema_from_yaml(config)

    print(f"📥 {config['client_name']} のデータ取り込み開始...")
    print(f"   スキーマ: {schema.simpleString()}")

    # PERMISSIVEモードで読み込み
    df = (
        spark.read.format(source["file_format"])
        .options(**source["options"])
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .schema(schema)
        .load(source["path"])
    )

    # メタデータ列を追加
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn(
        "_source_client", lit(client_id)
    )

    # ブロンズ層テーブルに書き込み
    table_name = f"demo_catalog.data_engineering.bronze_{client_id}"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    # 結果レポート
    total = df.count()
    corrupt = df.where("_corrupt IS NOT NULL").count()
    clean = total - corrupt
    print(f"   ✅ 完了: {total}行 (正常: {clean}, 不良データ: {corrupt})")
    print(f"   テーブル: {table_name}")
    return df


# COMMAND ----------

# ============================================================
# 実行: A社データ取り込み
# ============================================================
volume_path = "/Volumes/demo_catalog/data_engineering/raw_files"
config_a = load_yaml_config(f"{volume_path}/configs/company_a.yaml")
df_a = ingest_to_bronze(config_a)

print("\n--- A社 ブロンズ層 ---")
print(df_a)

# COMMAND ----------

# ============================================================
# 実行: B社データ取り込み
# ============================================================
config_b = load_yaml_config(f"{volume_path}/configs/company_b.yaml")
df_b = ingest_to_bronze(config_b)

print("\n--- B社 ブロンズ層 ---")
print(df_b)

# COMMAND ----------

# ============================================================
# 不良データの確認
# ============================================================
print("--- B社 不良データ (もしあれば _corrupt 列に格納) ---")
print(df_b.where("_corrupt IS NOT NULL"))

print("\n--- B社 正常データ ---")
print(df_b.where("_corrupt IS NULL"))

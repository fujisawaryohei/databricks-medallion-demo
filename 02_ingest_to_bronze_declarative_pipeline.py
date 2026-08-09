# Databricks notebook source
# ============================================================
# Pipeline (Declarative): 02_ingest_to_bronze
# 目的: YAML設定からスキーマを動的生成し、Lakeflow Declarative
#       Pipelines のブロンズ層テーブルとして「宣言的に」取り込む。
#
# ※ このファイルは通常のノートブックとして実行するのではなく、
#   Lakeflow Declarative Pipeline (旧 Delta Live Tables) の
#   ソースコードとして登録して実行します。
#   - spark セッションはパイプライン実行時に自動注入される（生成不要）
#   - dbutils も不要（YAMLはVolumeをPOSIXパスで直接読む）
#   - 出力先の catalog / schema はパイプライン設定の Target で決まる
#     （例: Target = demo_catalog.data_engineering に設定すると
#      bronze_company_a は demo_catalog.data_engineering.bronze_company_a になる）
#
# 命令的版 (02_ingest_to_bronze.py) との対応:
#   - 手続きで saveAsTable していた部分 → @dlt.table で宣言
#   - 実行順序の手動制御 → フレームワークが依存を解析して自動実行
# ============================================================

import dlt  # Lakeflow Declarative Pipelines API (旧 Delta Live Tables)
import yaml
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"


# --- YAML読み込みユーティリティ ---
def load_yaml_config(client_id: str) -> dict:
    """Volume上のYAML設定を読み込んでdictとして返す。

    /Volumes 配下はパイプライン実行クラスタ上でFUSEマウントされ、
    通常のローカルファイルとして open() で読める（dbutils不要）。
    """
    with open(f"{VOLUME_PATH}/configs/{client_id}.yaml") as f:
        return yaml.safe_load(f)


# --- StructType動的生成 (命令的版と同じロジック) ---
def build_schema_from_yaml(config: dict) -> StructType:
    """YAMLのschemaセクションからStructTypeを生成し、
    PERMISSIVEモード用の _corrupt 列を追加する。"""
    base_schema = StructType.fromJson(config["schema"])
    base_schema = base_schema.add(
        StructField(
            "_corrupt",
            StringType(),
            True,
            {"comment": "invalid rows captured by PERMISSIVE mode"},
        )
    )
    return base_schema


# --- ブロンズ層テーブルを宣言する高階関数 ---
def define_bronze_table(client_id: str):
    """client_id ごとに @dlt.table を1つ宣言する。

    命令的版では for ループで各社を順に saveAsTable していたが、
    宣言的版では「テーブル定義」を登録するだけで、実行・順序・
    再試行はフレームワークが受け持つ。
    """
    config = load_yaml_config(client_id)
    schema = build_schema_from_yaml(config)
    source = config["source"]

    @dlt.table(
        name=f"bronze_{client_id}",
        comment=f"{config['client_name']} raw ingestion (PERMISSIVE mode)",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        # PERMISSIVEモードで読み込み、不良行は _corrupt 列に隔離
        df = (
            spark.read.format(source["file_format"])  # noqa: F821 (sparkは自動注入)
            .options(**source["options"])
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt")
            .schema(schema)
            .load(source["path"])
        )
        # メタデータ列を付与
        return df.withColumn("_ingested_at", current_timestamp()).withColumn(
            "_source_client", lit(client_id)
        )

    return _bronze


# --- 各社のブロンズテーブルを宣言 ---
# ここを呼ぶと @dlt.table が登録される（この時点では実行されない。
# パイプライン実行時にフレームワークが評価・実行する）
define_bronze_table("company_a")
define_bronze_table("company_b")


# ============================================================
# 補足: 増分取り込みにしたい場合 (Auto Loader)
# ------------------------------------------------------------
# 上は命令的版と同じ「バッチ read」だが、宣言的パイプラインでは
# 到着ファイルを増分検知するストリーミング取り込みが定石。
# その場合は _bronze() を以下のように書き換える:
#
#   @dlt.table(name=f"bronze_{client_id}")
#   def _bronze():
#       return (
#           spark.readStream.format("cloudFiles")
#           .option("cloudFiles.format", source["file_format"])
#           .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schema/{client_id}")
#           .options(**source["options"])
#           .schema(schema)
#           .load(source["path"])
#       )
#
# これで新規ファイルだけが増分で取り込まれる（checkpoint等は
# パイプラインが自動管理）。
# ============================================================

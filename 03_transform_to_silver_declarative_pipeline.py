# Databricks notebook source
# ============================================================
# Pipeline (Declarative): 03_transform_to_silver
# 目的: ブロンズ層から YAML設定のカラムマッピング＆変換ルールを
#       適用し、共通データモデル(CDM)としてシルバー層へ統合する。
#       宣言的パイプラインの目玉である「データ品質エクスペクテーション」
#       (@dlt.expect 系) で不良データ検出を宣言的に行う。
#
# ※ 02_ingest_to_bronze_declarative_pipeline.py と同じ
#   Lakeflow Declarative Pipeline に含めて実行する。
#   silver_* が bronze_* に依存していることをフレームワークが
#   自動解析し、bronze → silver の順で実行される
#   （命令的版のように実行順序を手で並べる必要がない）。
#
# 命令的版 (03_transform_to_silver.py) との対応:
#   - .where("_corrupt IS NULL") の手動フィルタ → @dlt.expect_or_drop で宣言
#   - 末尾の groupBy / NULLレポートの display 診断 → エクスペクテーションの
#     品質メトリクスとしてパイプラインUIに自動集計される
# ============================================================

import dlt
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

VOLUME_PATH = "/Volumes/demo_catalog/data_engineering/raw_files"

# CDM (共通データモデル) の最終列セット
CDM_COLUMNS = [
    "employee_id",
    "base_salary",
    "department",
    "join_date",
    "allowed_roles",
    "source_client",
    "_transformed_at",
]


def load_yaml_config(client_id: str) -> dict:
    """Volume上のYAML設定を読み込む (02と同じユーティリティ)"""
    with open(f"{VOLUME_PATH}/configs/{client_id}.yaml") as f:
        return yaml.safe_load(f)


def with_cdm_metadata(df, config):
    """権限メタデータ・変換時刻を付与し、CDM列だけに整形する共通処理"""
    permissions = config["permission_metadata"]
    return (
        df.withColumn("allowed_roles", lit(",".join(permissions["allowed_roles"])))
        .withColumn("source_client", lit(config["client_id"]))
        .withColumn("_transformed_at", current_timestamp())
        .select(*CDM_COLUMNS)
    )


# ============================================================
# A社 シルバー: カラムリネームのみ
# ============================================================
@dlt.table(
    name="silver_company_a",
    comment="A社 CDM (rename only)",
    table_properties={"quality": "silver"},
)
# 宣言的な品質チェック: employee_id が無い行は破棄 / 給与欠損は警告のみ
@dlt.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
@dlt.expect("has_base_salary", "base_salary IS NOT NULL")
def silver_company_a():
    config = load_yaml_config("company_a")
    mapping = config["column_mapping"]

    # ブロンズ層をパイプライン内参照。PERMISSIVEで隔離した不良行を除外
    df = dlt.read("bronze_company_a").where("_corrupt IS NULL")

    # カラムマッピング適用
    for src_col, dst_col in mapping.items():
        df = df.withColumnRenamed(src_col, dst_col)

    return with_cdm_metadata(df, config)


# ============================================================
# B社 シルバー: 文字列→数値変換・日本語→英語マッピング・日付変換
# ============================================================
@dlt.table(
    name="silver_company_b",
    comment="B社 CDM (clean & normalize)",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
@dlt.expect("has_base_salary", "base_salary IS NOT NULL")  # "非公開"→null を警告集計
@dlt.expect("has_join_date", "join_date IS NOT NULL")  # invalid_date→null を警告集計
def silver_company_b():
    config = load_yaml_config("company_b")
    mapping = config["column_mapping"]
    transforms = config["transformations"]
    dept_map = transforms["department"]["mapping"]

    df = dlt.read("bronze_company_b").where("_corrupt IS NULL")

    # カラムリネーム
    for src_col, dst_col in mapping.items():
        df = df.withColumnRenamed(src_col, dst_col)

    # base_salary: ¥記号・カンマ除去 → 数値化 (変換失敗はnull)
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

    return with_cdm_metadata(df, config)


# ============================================================
# CDM統合: 各社シルバーを union してシルバー層CDMを宣言
# ============================================================
@dlt.table(
    name="silver_cdm",
    comment="全クライアント統合の共通データモデル (CDM)",
    table_properties={"quality": "silver"},
)
def silver_cdm():
    # silver_company_a / silver_company_b への依存を
    # フレームワークが自動解析し、両者の完成後に実行される
    df_a = dlt.read("silver_company_a")
    df_b = dlt.read("silver_company_b")
    return df_a.unionByName(df_b)


# ============================================================
# 補足: 命令的版の末尾にあった診断 (groupBy件数 / NULLレポート) について
# ------------------------------------------------------------
# それらの display() 診断は、宣言的版では上の @dlt.expect 系が
# 生成する「データ品質メトリクス」として Pipeline UI に自動集計される
# (期待に合致した行数 / ドロップ行数 / 警告行数 が可視化される)。
#
# 集計テーブルとして残したい場合は、ゴールド層として宣言できる:
#
#   @dlt.table(name="gold_dept_summary", comment="部門別サマリ")
#   def gold_dept_summary():
#       from pyspark.sql.functions import count
#       return (
#           dlt.read("silver_cdm")
#           .groupBy("source_client", "department")
#           .agg(count("*").alias("record_count"))
#       )
# ============================================================

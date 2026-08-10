"""シルバー層の変換ロジック(純関数)。

各 transform_* は「クレンジング済み(_corrupt除外済み)の DataFrame と config」を
受け取り、CDM(共通データモデル)形式の DataFrame を返す。
_corrupt の除外は呼び出し側で行う(命令的版は .where、宣言的版は @dp.expect)。
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    regexp_replace,
    to_date,
    when,
)
from pyspark.sql.types import IntegerType

# 全クライアント共通の最終列セット(CDM)
CDM_COLUMNS = [
    "employee_id",
    "base_salary",
    "department",
    "join_date",
    "allowed_roles",
    "source_client",
    "_transformed_at",
]


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """column_mapping に従って列名を一括リネームする。"""
    for src_col, dst_col in mapping.items():
        df = df.withColumnRenamed(src_col, dst_col)
    return df


def add_cdm_metadata(df: DataFrame, config: dict) -> DataFrame:
    """権限メタデータ・変換時刻を付与し、CDM列だけに整形する共通処理。"""
    permissions = config["permission_metadata"]
    return (
        df.withColumn("allowed_roles", lit(",".join(permissions["allowed_roles"])))
        .withColumn("source_client", lit(config["client_id"]))
        .withColumn("_transformed_at", current_timestamp())
        .select(*CDM_COLUMNS)
    )


def transform_company_a(df: DataFrame, config: dict) -> DataFrame:
    """A社: カラムリネームのみで CDM へマッピングする。"""
    df = rename_columns(df, config["column_mapping"])
    return add_cdm_metadata(df, config)


def transform_company_b(df: DataFrame, config: dict) -> DataFrame:
    """B社: ¥除去→数値化、日本語→英語マッピング、日付変換を行い CDM 化する。"""
    df = rename_columns(df, config["column_mapping"])

    # base_salary: ¥記号・カンマ除去 → 数値化 (変換失敗はnull)
    df = df.withColumn(
        "base_salary",
        regexp_replace(col("base_salary"), "[¥,]", "").cast(IntegerType()),
    )

    # department: 日本語 → 英語マッピング
    dept_map = config["transformations"]["department"]["mapping"]
    dept_expr = col("department")
    for jp, en in dept_map.items():
        dept_expr = when(col("department") == jp, lit(en)).otherwise(dept_expr)
    df = df.withColumn("department", dept_expr)

    # join_date: yyyy/MM/dd → date型
    df = df.withColumn("join_date", to_date(col("join_date"), "yyyy/MM/dd"))

    return add_cdm_metadata(df, config)

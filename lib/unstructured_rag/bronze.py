"""ブロンズ層: 生バイナリ取り込み(純関数)。

取り込みモードを2種類サポートし、config で切り替える:
  - "batch"      : UC Volumes 等に配置済みファイルを一括読み込み(動作検証向け)
  - "autoloader" : Auto Loader(cloudFiles)で増分ストリーミング取り込み(本番向け)

どちらも binaryFile 形式で path / modificationTime / length / content を返すため、
後続の to_bronze() は共通で使える。
spark は引数で受け取り、DataFrame を返すだけ(書き込み/テーブル化は呼び出し側)。
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

from lib.unstructured_rag.config import CORRUPT_COLUMN

# Bronze層の最終列(スキーマ例に対応)
BRONZE_COLUMNS = ["file_path", "binary_content", "ingest_timestamp", CORRUPT_COLUMN]

# サポートする取り込みモード
INGEST_MODES = ("batch", "autoloader")


def read_binary_batch(spark, source_path: str, path_glob_filter: str | None = None) -> DataFrame:
    """UC Volumes 等に配置済みのファイルを一括(バッチ)で読み込む。

    Auto Loader を使わずその場のファイルを全件読むため、動作検証に向く。
    binaryFile 形式で path / modificationTime / length / content を返す。
    """
    reader = spark.read.format("binaryFile")
    if path_glob_filter:
        reader = reader.option("pathGlobFilter", path_glob_filter)
    return reader.load(source_path)


def read_binary_stream(spark, source_path: str, path_glob_filter: str | None = None) -> DataFrame:
    """Auto Loader(cloudFiles)で生バイナリを増分ストリーミング取り込みする(本番向け)。

    cloudFiles.format=binaryFile は path / modificationTime / length / content を返す。
    ※ cloudFiles は Databricks 固有機能。ローカルConnect/OSS Sparkでは動かない。
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
    )
    if path_glob_filter:
        reader = reader.option("pathGlobFilter", path_glob_filter)
    return reader.load(source_path)


def read_binary(
    spark,
    source_path: str,
    mode: str = "batch",
    path_glob_filter: str | None = None,
) -> DataFrame:
    """取り込みモードに応じて batch / autoloader を切り替えて読み込む。

    Args:
        mode: "batch"(Volumes一括) または "autoloader"(増分ストリーミング)
    """
    if mode == "autoloader":
        return read_binary_stream(spark, source_path, path_glob_filter)
    if mode == "batch":
        return read_binary_batch(spark, source_path, path_glob_filter)
    raise ValueError(f"unknown ingest mode: {mode!r} (expected one of {INGEST_MODES})")


def to_bronze(df: DataFrame) -> DataFrame:
    """binaryFile の生カラムを Bronze スキーマに整形しメタデータを付与する。

    破損検知(パース失敗)は Silver 層で行うため、Bronze の _corrupt は
    プレースホルダ(null)として保持する。batch / autoloader 共通で使える。
    """
    return (
        df.select(
            col("path").alias("file_path"),
            col("content").alias("binary_content"),
            current_timestamp().alias("ingest_timestamp"),
        )
        .withColumn(CORRUPT_COLUMN, lit(None).cast("string"))
    )

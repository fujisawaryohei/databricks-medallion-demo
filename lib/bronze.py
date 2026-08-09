"""ブロンズ層の取り込みロジック (純関数)。

spark は引数で受け取り、DataFrame を返すだけ。書き込み (saveAsTable) や
@dlt.table でのテーブル化は呼び出し側 (Notebook/Pipeline) の責務。
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

from lib.config import CORRUPT_COLUMN


def read_source(spark, source: dict, schema) -> DataFrame:
    """YAMLの source 設定に従い、PERMISSIVEモードで生データを読み込む。

    不良行は破棄せず CORRUPT_COLUMN に隔離される。
    """
    return (
        spark.read.format(source["file_format"])
        .options(**source["options"])
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(schema)
        .load(source["path"])
    )


def add_ingest_metadata(df: DataFrame, client_id: str) -> DataFrame:
    """取り込み時刻・ソースクライアントのメタデータ列を付与する。"""
    return df.withColumn("_ingested_at", current_timestamp()).withColumn(
        "_source_client", lit(client_id)
    )

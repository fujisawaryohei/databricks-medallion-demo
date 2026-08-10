"""YAML設定のパースと、Sparkスキーマの動的生成。

I/O(ファイル読み取り)は実行コンテキストで異なる(ローカルは dbutils、
Pipelineは open)ため、ここには「文字列 → dict」「dict → StructType」の
純ロジックだけを置き、実際の読み取りは呼び出し側(Notebook/Pipeline)が行う。
"""

import yaml  # type: ignore[import]
from pyspark.sql.types import StringType, StructField, StructType

# PERMISSIVEモードで不良行を隔離するためのカラム名
CORRUPT_COLUMN = "_corrupt"


def parse_yaml_config(content: str) -> dict:
    """YAML文字列を dict にパースする(純関数)。"""
    return yaml.safe_load(content)


def build_schema_from_yaml(config: dict) -> StructType:
    """YAMLの schema セクションから StructType を生成し、
    PERMISSIVEモード用の _corrupt 列を追加する。"""
    base_schema = StructType.fromJson(config["schema"])
    return base_schema.add(
        StructField(
            CORRUPT_COLUMN,
            StringType(),
            True,
            {"comment": "invalid rows captured by PERMISSIVE mode"},
        )
    )

"""パーサー/チャンキングのYAML設定パースと、共通定数・ヘルパー。

structured_yaml と同様、I/O(ファイル読み取り)は呼び出し側に任せ、
ここでは「文字列 → dict」と、設定値を引き出す純ヘルパーを提供する。
"""

import os

import yaml

# 破損ファイル/抽出失敗の退避用カラム
CORRUPT_COLUMN = "_corrupt"

# 拡張子 → source_type の対応
_EXT_TO_SOURCE_TYPE = {
    ".pdf": "pdf",
    ".pptx": "pptx",
    ".ppt": "pptx",
    ".md": "md",
    ".markdown": "md",
}

# サポートするソース種別
SUPPORTED_SOURCE_TYPES = ("pdf", "pptx", "md")


def parse_yaml_config(content: str) -> dict:
    """YAML文字列を dict にパースする(純関数)。"""
    return yaml.safe_load(content)


def detect_source_type(file_path: str) -> str | None:
    """ファイルパスの拡張子から source_type を判定する。

    未対応拡張子の場合は None を返す。
    """
    ext = os.path.splitext(file_path)[1].lower()
    return _EXT_TO_SOURCE_TYPE.get(ext)


def get_chunk_strategy(source_type: str, config: dict) -> dict:
    """source_type に対応するチャンキング戦略設定を返す。

    config 例:
      chunking:
        md:   {strategy: markdown_header, ...}
        pdf:  {strategy: paragraph, max_chars: 1000, overlap: 100}
        pptx: {strategy: paragraph, max_chars: 1000, overlap: 100}
        default: {strategy: paragraph, max_chars: 1000, overlap: 100}
    """
    chunking = config.get("chunking", {})
    return chunking.get(source_type) or chunking.get("default") or {
        "strategy": "paragraph",
        "max_chars": 1000,
        "overlap": 100,
    }

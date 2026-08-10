"""ゴールド層: チャンク分割 + アクセス制御メタデータ付与。

- chunk_markdown / chunk_by_paragraph / chunk_text は純関数(テスト可能)。
- to_gold_chunks は Silver DataFrame を受け取り、チャンク行に展開する Spark ヘルパー。

チャンキング戦略(config駆動):
  - md         : 見出し構造を保持する形式固有分割(chunk_markdown)
  - pdf / pptx : 段落ベース分割(chunk_by_paragraph)
"""

import hashlib
import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, posexplode, udf
from pyspark.sql.types import ArrayType, StringType


# ============================================================
# 純関数(テスト対象)
# ============================================================
def chunk_markdown(text: str) -> list[str]:
    """見出し(#)構造を保持して分割する。各チャンクに見出しパスを前置する。"""
    if not text:
        return []
    chunks: list[str] = []
    heading_stack: list[str] = []
    buffer: list[str] = []

    def flush():
        body = "\n".join(buffer).strip()
        if body:
            prefix = " > ".join(heading_stack)
            chunks.append(f"{prefix}\n{body}" if prefix else body)

    for line in text.splitlines():
        m = re.match(r"^(#{1,6})\s+(.*)$", line)
        if m:
            flush()
            buffer = []
            level = len(m.group(1))
            heading_stack = heading_stack[: level - 1]
            heading_stack.append(m.group(2).strip())
        else:
            buffer.append(line)
    flush()
    return chunks


def chunk_by_paragraph(text: str, max_chars: int = 1000, overlap: int = 100) -> list[str]:
    """段落(空行区切り)を、max_chars を上限に貪欲にまとめる。overlapで文脈を重ねる。"""
    if not text:
        return []
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    chunks: list[str] = []
    current = ""
    for para in paragraphs:
        if current and len(current) + len(para) + 2 > max_chars:
            chunks.append(current.strip())
            # 直前チャンクの末尾 overlap 文字を次チャンクの先頭に重ねる
            current = (current[-overlap:] if overlap else "") + "\n\n" + para
        else:
            current = f"{current}\n\n{para}" if current else para
    if current.strip():
        chunks.append(current.strip())
    return chunks


def chunk_text(text: str, source_type: str, strategy: dict) -> list[str]:
    """source_type と戦略設定に応じてチャンク分割する(純関数・ディスパッチ)。"""
    name = strategy.get("strategy", "paragraph")
    if name == "markdown_header" or source_type == "md":
        return chunk_markdown(text)
    return chunk_by_paragraph(
        text,
        max_chars=strategy.get("max_chars", 1000),
        overlap=strategy.get("overlap", 100),
    )


def make_chunk_id(document_id: str, chunk_index: int) -> str:
    """document_id + chunk_index から決定的な chunk_id を生成する。"""
    raw = f"{document_id}:{chunk_index}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


# ============================================================
# Spark ヘルパー
# ============================================================
def to_gold_chunks(df: DataFrame, config: dict) -> DataFrame:
    """Silver(document_id, extracted_text, source_type) をチャンク行に展開する。

    アクセス制御メタデータ(allowed_roles / department)は config の
    permission_metadata から付与(継承)する。
    """
    chunking = config.get("chunking", {})
    default_strategy = chunking.get("default", {"strategy": "paragraph", "max_chars": 1000, "overlap": 100})
    permissions = config.get("permission_metadata", {})
    allowed_roles = ",".join(permissions.get("allowed_roles", []))
    department = permissions.get("department", "")

    def _chunk(text: str, source_type: str) -> list[str]:
        strategy = chunking.get(source_type, default_strategy)
        return chunk_text(text or "", source_type, strategy)

    chunk_udf = udf(_chunk, ArrayType(StringType()))
    chunk_id_udf = udf(make_chunk_id, StringType())

    exploded = (
        df.where(col("extracted_text").isNotNull())
        .withColumn("_chunks", chunk_udf(col("extracted_text"), col("source_type")))
        .select(
            "document_id",
            "source_type",
            posexplode(col("_chunks")).alias("chunk_index", "chunk_text"),
        )
    )

    return (
        exploded.withColumn("chunk_id", chunk_id_udf(col("document_id"), col("chunk_index")))
        .withColumn("allowed_roles", lit(allowed_roles))
        .withColumn("department", lit(department))
        .select(
            "chunk_id",
            "document_id",
            "chunk_text",
            "allowed_roles",
            "department",
            "chunk_index",
        )
    )

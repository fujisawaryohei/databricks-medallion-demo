# Databricks メダリオンアーキテクチャ デモ

Databricks 上でメダリオンアーキテクチャ（Bronze / Silver / Gold）を実装するデモです。
**2種類のパイプライン**を、それぞれ **命令的（PySpark）版** と **宣言的（Lakeflow Spark Declarative Pipelines）版** の両方で提供します。

> 宣言的版は、Apache Spark 本体に取り込まれた新API `from pyspark import pipelines as dp` を使用します（旧 `dlt` モジュールの後継）。ストリーミングテーブルは `@dp.table`、バッチのマテリアライズドビューは `@dp.materialized_view`、品質チェックは `@dp.expect_or_drop` 等で記述します。

| # | パイプライン | 対象データ | 概要 |
|:--|:--|:--|:--|
| ① | **structured_yaml** | 構造化（CSV / Excel） | YAML設定駆動でスキーマ定義・型変換・CDM統合を行う |
| ② | **unstructured_rag** | 非構造化（PDF / PPTX / MD） | テキスト抽出・チャンク分割・ベクトル検索同期でRAG基盤を構築する |

どちらのロジックも `lib/` パッケージ（純関数）に集約し、`pipelines/` 配下のNotebook（オーケストレーション層）から呼び出します。

---

## ディレクトリ構成（パイプラインベース）

```
databricks-medallion-demo/
├── lib/                          # 再利用ロジック（純関数パッケージ / File）
│   ├── structured_yaml/                #   PL① のロジック
│   │   ├── config.py                   #     YAMLパース / StructType動的生成
│   │   ├── bronze.py                   #     PERMISSIVE取り込み
│   │   └── silver.py                   #     カラムマッピング・型変換・CDM統合
│   └── unstructured_rag/               #   PL② のロジック
│       ├── config.py                   #     パーサー / チャンキング設定
│       ├── bronze.py                   #     Auto Loader バイナリ取り込み
│       ├── silver.py                   #     テキスト抽出・クレンジング・PII・重複排除
│       ├── gold.py                     #     チャンク分割・権限メタ付与
│       └── vector_search.py            #     AI Search（Vector Search）同期
│
├── pipelines/                          # オーケストレーション（Notebook）/ パイプライン単位
│   ├── structured_yaml/
│   │   ├── setup/                      #   前提セットアップ
│   │   │   ├── 00_setup_sample_data.py #     サンプルCSVをVolumeに配置
│   │   │   └── 01_create_yaml_configs.py#    クライアント別YAML設定を配置
│   │   ├── pyspark/                    #   命令的版
│   │   │   ├── 02_ingest_to_bronze.py
│   │   │   └── 03_transform_to_silver.py
│   │   └── declarative/                        #   宣言的版
│   │       ├── 02_ingest_to_bronze.py
│   │       └── 03_transform_to_silver.py
│   └── unstructured_rag/
│       ├── setup/
│       │   ├── 00_setup_sample_docs.py #     サンプルPDF/PPTX/MDをVolumeに配置
│       │   └── 01_create_parsing_configs.py# パーサー/チャンキングYAML設定を配置
│       ├── pyspark/                    #   命令的版
│       │   ├── 02_ingest_to_bronze.py  #     Auto Loader バイナリ取り込み
│       │   ├── 03_extract_to_silver.py #     テキスト抽出・クレンジング
│       │   └── 04_chunk_to_gold.py     #     チャンク分割・権限メタ付与
│       └── declarative/                        #   宣言的版（Bronze→Silver→Goldまで）
│           ├── 02_ingest_to_bronze.py
│           ├── 03_extract_to_silver.py
│           └── 04_chunk_to_gold.py
│
├── playground/
│   └── dataframe_api_playground.py     # DataFrame API 学習用サンプル
├── tests/                              # lib/ 純関数のユニットテスト
│   ├── test_structured_yaml.py
│   └── test_unstructured_rag.py
│
├── databricks.yml                      # DABs: パイプライン/ジョブ/スケジュール定義
├── pyproject.toml                      # 依存管理（uv）
├── uv.lock
├── main.py
└── README.md
```

### 設計の考え方

- **ロジックとオーケストレーションの分離**: `lib/`（何をするか＝純関数）と `pipelines/`（どう繋ぐか＝Notebook）を分離。純関数は `spark`/`df` を引数で受け取り、**ローカルでユニットテスト可能**。
- **パイプライン単位で自己完結**: `lib/<pipeline>/` と `pipelines/<pipeline>/` が対になる。パイプラインを追加する時は同じ2箇所に足すだけ。
- **命令的／宣言的を同じロジックで共有**: `pyspark/` も `declarative/` も同じ `lib/` を import する。ロジックの重複なし。
- **Vector Search 同期は今回のスコープ外（将来）**: Gold の Delta テーブルを対象にした SDK 操作でパラダイム非依存。現時点では Bronze→Silver→Gold まで実装し、インデックス同期は未実装（`lib/unstructured_rag/vector_search.py` は休眠）。

---

## パイプライン① structured_yaml（構造化データ / YAML駆動）

CSV（社員データ）を、YAML設定に定義したスキーマ・変換ルールに従って取り込み・整形する。

| 層 | 処理 | ロジック |
|:--|:--|:--|
| Bronze | YAML→StructType動的生成 → **PERMISSIVEモード**で取り込み（不良行は `_corrupt` に隔離） | `lib/structured_yaml/{config,bronze}.py` |
| Silver | カラムマッピング・型変換（¥除去・日付整形）・日本語→英語・**CDM統合** | `lib/structured_yaml/silver.py` |

- **A社**（英語カラム・クリーン）と **B社**（日本語カラム・不良データ混在）の2社を、共通データモデル（CDM）に統合。
- 宣言的版では不良データ検出を `@dp.expect` 系のデータ品質エクスペクテーションで宣言的に実施。

---

## パイプライン② unstructured_rag（非構造化データ / RAG基盤）

PDF / PPTX / MD ファイルから、AIエージェント向けのベクトル検索インデックスを構築する。

### 各層の役割

| 層 | 役割 | 主な実装 |
|:--|:--|:--|
| **Bronze**（Raw・生バイナリ） | クラウドストレージ上のPDF/PPTX/MDを、変換せずそのままDeltaに取り込む。**Auto Loader (`cloudFiles`)** で新着ファイルを増分検知 | `bronze.py` |
| **Silver**（Clean・ドキュメント単位テキスト） | 生バイナリから高品質テキストを抽出（`unstructured` / `PyPDF2` 等）。メタデータ付与・**重複排除**・**PIIフィルタリング** | `silver.py` |
| **Gold**（Chunk・検索モデリング用） | 長文を意味のあるチャンクに分割し、検索メタデータと紐付け | `gold.py` |
| **Vector Search 同期** | Gold の Delta テーブルをベクトルインデックスに継続同期 | `vector_search.py` |

想定スキーマ:
- Bronze: `file_path`, `binary_content`, `ingest_timestamp`, `_corrupt`
- Silver: `document_id`, `extracted_text`, `source_type`(pdf/pptx/md), `metadata`(map)
- Gold: `chunk_id`, `document_id`, `chunk_text`, `allowed_roles`, `department`, `chunk_index`

### チャンキング戦略（形式ごとに切替）

| 形式 | 戦略 |
|:--|:--|
| Markdown | 見出し構造を保持する **形式固有分割**（`MarkdownHeaderTextSplitter` 等） |
| PDF / PPTX | **セマンティックチャンク分割** または **段落ベース分割** |

### アクセス制御メタデータ

各チャンクに **権限属性（`allowed_roles` / `department`）** を付与（継承）し、Vector Search 検索時に `filters_json` でハイブリッドフィルタリングできるようにする。

---

## 運用・更新のベストプラクティス（データ一貫性）

- **原則: ブロンズ層への追記（Append-only）**。既存の Silver/Gold レコードを直接 `UPDATE` しない。
- 権限変更などの要求は、最新ドキュメント情報を **「変更イベント」としてBronzeへAppend**。
- ETLがこの変更を検知し、下流テーブルに **`MERGE INTO`（Upsert）** を適用して、Delta と Vector Search の状態を整合性高く更新する。

---

## 多層防御セキュリティ（Unity Catalog セーフガード）

AIエージェント側の `filters_json` 制御に不具合が生じても、DBレイヤーで機密データを保護する。

- **物理テーブルへの直接 `SELECT` をブロック**: エージェントのサービスプリンシパルに Gold 生テーブルを直接叩かせない。
- **動的ビュー（Dynamic View）**: `is_account_group_member` / `has_tag_value` を用いた動的ビューを Unity Catalog に作成し、経由アクセスさせて機密列・不要行を自動マスク。
- **最小権限**: 親コンテナ（Catalog/Schema）の `USE` と、動的ビューへの `SELECT` のみを明示付与。

---

## 開発〜本番デプロイのステップ

1. **ノートブックで対話検証**: 汎用クラスタ（またはサーバーレス）で、パーサー/チャンキングのロジックを対話的に調整。
2. **インフラのコード化**: Lakeflow Declarative Pipelines（旧DLT）のPythonコードに落とし込む（`pipelines/<pipeline>/declarative/`）。
3. **DABsでデプロイ**: Declarative Automation Bundles（旧 Asset Bundles）＋ `databricks.yml` で、ソースコードとスケジュール/コンピュート設定をセットにし、Git / CI/CD 経由で本番へデプロイ。
4. **本番実行（サーバーレス）**: 実行時にジョブクラスタが自動起動→実行→破棄でコスト最適化。

---

## セットアップ（ローカル開発）

```bash
# 依存を復元（uv）
uv sync            # 開発込み
uv sync --no-dev   # 本番相当（ipykernel等を除く）
```

- Python は `3.12`（接続先 DBR に合わせる。`.python-version` / `pyproject.toml` で固定）。
- 主要依存: `databricks-connect`（ローカル→クラスタ実行）、`pyyaml`。非構造化パイプライン用に `unstructured` / `pypdf` 等を追加予定。
- `lib/` パッケージは、Notebook 冒頭のブートストラップで repo root を `sys.path` に追加して import する（本番では wheel 化も検討）。

## import の仕組み

各Notebookは、サブフォルダから共通の `lib/` を import できるよう、冒頭で repo root を探して `sys.path` に追加する。

```python
from lib.unstructured_rag.silver import extract_text   # 例
```

---

## パイプライン一覧（実行順）

### ① structured_yaml
`setup/00 → setup/01 → 02_ingest_to_bronze → 03_transform_to_silver`

### ② unstructured_rag
`setup/00 → setup/01 → 02_ingest_to_bronze → 03_extract_to_silver → 04_chunk_to_gold`

各パイプラインとも **pyspark版 / declarative版** を用意。declarative版はパイプラインとして実行し、依存（Bronze→Silver→Gold）は自動解決される。

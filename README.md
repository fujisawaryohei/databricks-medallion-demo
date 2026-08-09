# Databricks Free Edition メダリオンアーキテクチャ デモ

YAML設定駆動によるスキーマ定義・適用の実装デモです。

## ノートブック一覧

| 順番 | ファイル | 内容 |
|:--|:--|:--|
| 1 | `00_setup_sample_data.py` | サンプルCSVデータをUnity Catalog Volumesに配置 |
| 2 | `01_create_yaml_configs.py` | クライアント別YAML設定ファイル（スキーマ定義・マッピングルール）を作成 |
| 3 | `02_ingest_to_bronze.py` | YAML→StructType動的生成 → PERMISSIVEモードでブロンズ層取り込み |
| 4 | `03_transform_to_silver.py` | カラムマッピング・型変換・日本語→英語変換 → CDM統合してシルバー層書き込み |

## Databricksへのインポート手順

1. [Databricks Free Edition](https://www.databricks.com/try-databricks) にログイン
2. ワークスペースの「Workspace」メニューを開く
3. 「Import」をクリック
4. このフォルダ内の `.py` ファイルを **番号順に** インポート
5. 各ノートブックを上から順に実行（`Run All`）

## 前提条件

- Databricks Free Edition のアカウント
- サーバーレスコンピュートまたはクラスタが起動済み
- `pyyaml` がクラスタにインストールされていること（なければ Notebook 02 の冒頭で `%pip install pyyaml`）

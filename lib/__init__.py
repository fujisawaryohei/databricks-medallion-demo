"""再利用可能なETLロジック（純関数パッケージ）。

パイプラインごとにサブパッケージを分ける:
  - lib.structured_yaml : 構造化データ(CSV/Excel)・YAML駆動パイプライン
  - lib.unstructured_rag: 非構造化データ(PDF/PPTX/MD)・RAG/ベクトル検索パイプライン

各関数は spark / DataFrame / bytes などを引数で受け取る純関数として実装し、
グローバルな spark / dbutils に依存しない = ローカルでユニットテスト可能。
オーケストレーション(書き込み・@dp.table)は pipelines/ 側の責務。
"""

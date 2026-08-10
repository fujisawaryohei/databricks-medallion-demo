"""非構造化データ(PDF/PPTX/MD)・RAG/ベクトル検索パイプラインのロジック。

Bronze(生バイナリ) → Silver(テキスト抽出/クレンジング) → Gold(チャンク/権限メタ)
→ Vector Search 同期、の各層の純ロジックを提供する。

パーサー系の外部ライブラリ(pypdf / python-pptx)や databricks-vectorsearch は、
モジュール読込時ではなく関数呼び出し時に遅延importする(未インストール環境でも
純ロジックのテストが可能)。
"""

"""再利用可能なETLロジック (純関数モジュール)。

命令的Notebook (02/03) と 宣言的Pipeline (02/03_declarative) の
両方から import して使う共通ロジックを置く。

各関数は spark セッションや DataFrame を引数で受け取る純関数として実装し、
グローバルな spark / dbutils に依存しない = ローカルでユニットテスト可能。
"""

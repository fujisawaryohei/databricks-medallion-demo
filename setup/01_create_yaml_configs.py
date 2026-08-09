# Databricks notebook source
# ============================================================
# Notebook: setup/01_create_yaml_configs
# 目的: クライアントごとのスキーマ定義YAMLを Volume に配置する。
#       pyspark版・dlt版どちらのパイプラインでも共通の前提セットアップ。
# ============================================================

# COMMAND ----------

# --- ローカル実行用セットアップ (Databricks上では不要) ---
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

volume_path = "/Volumes/demo_catalog/data_engineering/raw_files"

# COMMAND ----------

# --- A社用 YAML設定 ---
company_a_yaml = """
client_id: company_a
client_name: "A社"
source:
  file_format: csv
  path: "/Volumes/demo_catalog/data_engineering/raw_files/company_a/"
  options:
    header: "true"
    encoding: "UTF-8"

# スキーマ定義: Spark StructType JSON形式
schema:
  type: struct
  fields:
    - name: Emp_No
      type: string
      nullable: false
    - name: BasePay
      type: integer
      nullable: true
    - name: Department
      type: string
      nullable: true
    - name: JoinDate
      type: date
      nullable: true

# ブロンズ → シルバー変換時のカラムマッピング
column_mapping:
  Emp_No: employee_id
  BasePay: base_salary
  Department: department
  JoinDate: join_date

# 権限メタデータ (AI Search用の土台)
permission_metadata:
  allowed_roles:
    - analyst
    - manager
  department_scope: "from_data"
"""

dbutils.fs.put(f"{volume_path}/configs/company_a.yaml", company_a_yaml, overwrite=True)
print("✅ A社 YAML設定を作成しました")

# COMMAND ----------

# --- B社用 YAML設定 ---
company_b_yaml = """
client_id: company_b
client_name: "B社"
source:
  file_format: csv
  path: "/Volumes/demo_catalog/data_engineering/raw_files/company_b/"
  options:
    header: "true"
    encoding: "UTF-8"

schema:
  type: struct
  fields:
    - name: "社員番号"
      type: string
      nullable: false
    - name: "基本給"
      type: string
      nullable: true
    - name: "部門"
      type: string
      nullable: true
    - name: "入社日"
      type: string
      nullable: true

column_mapping:
  "社員番号": employee_id
  "基本給": base_salary
  "部門": department
  "入社日": join_date

# B社固有の変換ルール
transformations:
  base_salary:
    - strip_chars: "¥,"
    - cast: integer
  join_date:
    - date_format: "yyyy/MM/dd"
  department:
    mapping:
      "営業部": Sales
      "開発部": Engineering
      "人事部": HR

permission_metadata:
  allowed_roles:
    - analyst
  department_scope: "from_data"
"""

dbutils.fs.put(f"{volume_path}/configs/company_b.yaml", company_b_yaml, overwrite=True)
print("✅ B社 YAML設定を作成しました")

# COMMAND ----------

# --- 確認 ---
for f in dbutils.fs.ls(f"{volume_path}/configs/"):
    print(f.name, f.size)

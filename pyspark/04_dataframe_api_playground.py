# Databricks notebook source
# ============================================================
# Notebook: 04_dataframe_api_playground
# 目的: Table生成 → DataFrame API でクエリ → 結果表示 を
#       一通り体験する学習用サンプル (PySparkオンリー / dltなし)
#
# 実行方法:
#   - ローカル(VSCode + Databricks Connect): 下のセットアップセルが必要
#   - Databricks上: セットアップセルは不要 (spark が自動注入される)
#
# 表示について:
#   - ローカルでは display() が使えないため .show() を使用
#   - Databricks上なら display(df) でリッチな表表示になる
# ============================================================

# COMMAND ----------

# --- ローカル実行用セットアップ (Databricks上では不要) ---
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import (
    avg,
    col,
    count,
    desc,
    lit,
    round as spark_round,
    upper,
    when,
)

# 出力先 (00で作成済みの catalog/schema を利用)
CATALOG = "demo_catalog"
SCHEMA = "data_engineering"
EMP_TABLE = f"{CATALOG}.{SCHEMA}.dfapi_employees"
DEPT_TABLE = f"{CATALOG}.{SCHEMA}.dfapi_departments"

# COMMAND ----------

# ============================================================
# STEP 1: サンプルデータから Delta テーブルを生成する
# ============================================================

# --- 従業員テーブル ---
emp_data = [
    # (emp_id, name, dept, base_salary, age)
    ("E001", "Sato", "Sales", 350000, 28),
    ("E002", "Suzuki", "Engineering", 520000, 35),
    ("E003", "Takahashi", "Sales", 410000, 42),
    ("E004", "Tanaka", "Engineering", 480000, 31),
    ("E005", "Ito", "HR", 380000, 39),
    ("E006", "Watanabe", "Engineering", 610000, 45),
    ("E007", "Yamamoto", "Sales", 290000, 24),
    ("E008", "Nakamura", "HR", 430000, 50),
]
emp_columns = ["emp_id", "name", "dept", "base_salary", "age"]

df_emp = spark.createDataFrame(emp_data, emp_columns)

# Delta テーブルとして保存 (saveAsTable はアクション → ここで実行される)
df_emp.write.format("delta").mode("overwrite").saveAsTable(EMP_TABLE)
print(f"✅ 従業員テーブル作成: {EMP_TABLE}")

# --- 部門マスタテーブル (join用) ---
dept_data = [
    # (dept, dept_name_jp, location)
    ("Sales", "営業部", "Tokyo"),
    ("Engineering", "開発部", "Osaka"),
    ("HR", "人事部", "Tokyo"),
]
dept_columns = ["dept", "dept_name_jp", "location"]

df_dept = spark.createDataFrame(dept_data, dept_columns)
df_dept.write.format("delta").mode("overwrite").saveAsTable(DEPT_TABLE)
print(f"✅ 部門マスタ作成: {DEPT_TABLE}")

# COMMAND ----------

# ============================================================
# STEP 2: テーブルを DataFrame として読み込む
# ============================================================
# spark.table() でテーブルを DataFrame 化 (まだデータは動かない=遅延評価)
df = spark.table(EMP_TABLE)

print("--- スキーマ確認 ---")
df.printSchema()

print("--- 全件表示 (show はアクション → ここで実行される) ---")
df.show()

# COMMAND ----------

# ============================================================
# STEP 3: select / where で絞り込み
# ============================================================
# 特定列だけ選び、給与40万以上に絞る
result = df.select("emp_id", "name", "dept", "base_salary").where(
    col("base_salary") >= 400000
)

print("--- 給与40万以上 (select + where) ---")
result.show()

# COMMAND ----------

# ============================================================
# STEP 4: withColumn で派生列を追加
# ============================================================
# 給与を千円単位に変換した列と、年代ラベル列を追加
enriched = (
    df.withColumn("salary_k", spark_round(col("base_salary") / 1000, 0))
    .withColumn(
        "generation",
        when(col("age") < 30, lit("20s"))
        .when(col("age") < 40, lit("30s"))
        .when(col("age") < 50, lit("40s"))
        .otherwise(lit("50s+")),
    )
    .withColumn("dept_upper", upper(col("dept")))
)

print("--- 派生列を追加 (withColumn) ---")
enriched.select("name", "base_salary", "salary_k", "age", "generation", "dept_upper").show()

# COMMAND ----------

# ============================================================
# STEP 5: groupBy + agg で集計
# ============================================================
# 部門別に「人数・平均給与・最高給与」を集計
from pyspark.sql.functions import max as spark_max

summary = (
    df.groupBy("dept")
    .agg(
        count("*").alias("headcount"),
        spark_round(avg("base_salary"), 0).alias("avg_salary"),
        spark_max("base_salary").alias("max_salary"),
    )
    .orderBy(desc("avg_salary"))  # 平均給与の降順に並べ替え
)

print("--- 部門別サマリ (groupBy + agg + orderBy) ---")
summary.show()

# COMMAND ----------

# ============================================================
# STEP 6: join で2テーブルを結合
# ============================================================
# 従業員テーブルに部門マスタを結合し、部門の日本語名・拠点を付与
df_dept = spark.table(DEPT_TABLE)

joined = df.join(df_dept, on="dept", how="inner").select(
    "emp_id", "name", "dept", "dept_name_jp", "location", "base_salary"
)

print("--- 従業員 × 部門マスタ (inner join) ---")
joined.orderBy("dept", "emp_id").show()

# COMMAND ----------

# ============================================================
# STEP 7: DataFrame API と SQL は相互変換できる (同じ結果)
# ============================================================
# 同じ「部門別平均給与」を SQL でも書いてみる
df.createOrReplaceTempView("emp_view")

sql_result = spark.sql(
    """
    SELECT dept, ROUND(AVG(base_salary), 0) AS avg_salary
    FROM emp_view
    GROUP BY dept
    ORDER BY avg_salary DESC
    """
)

print("--- 同じ集計を SQL で (DataFrame APIと等価) ---")
sql_result.show()

# COMMAND ----------

# ============================================================
# STEP 8: 加工結果を新しいテーブルとして書き出す
# ============================================================
# STEP5 のサマリを Delta テーブルとして保存 (write はアクション)
summary.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.dfapi_dept_summary"
)
print(f"✅ 集計結果を保存: {CATALOG}.{SCHEMA}.dfapi_dept_summary")

# 読み戻して確認
spark.table(f"{CATALOG}.{SCHEMA}.dfapi_dept_summary").show()

# COMMAND ----------

# ============================================================
# (任意) 後片付け: 作成したテーブルを削除する場合
# ============================================================
# spark.sql(f"DROP TABLE IF EXISTS {EMP_TABLE}")
# spark.sql(f"DROP TABLE IF EXISTS {DEPT_TABLE}")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.dfapi_dept_summary")
# print("🗑️ テーブルを削除しました")

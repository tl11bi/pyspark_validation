import os, sys

from validators.dataframe_validator import SparkDataValidator

# 1) Force Spark to use your current interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2) (Optional) verify
print("Using Python:", sys.executable)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("local-test")
    .master("local[*]")
    # You can also set via Spark conf (redundant with envs but fine):
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)


csv_path = "tests/data/sample_csv_data.csv"
rules_path = "tests/rules/sample_csv_rules/rules.json"

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")   # OK for tests; for prod, define schema explicitly
      .csv(csv_path))

with open(rules_path, "r", encoding="utf-8") as f:
    rules_text = f.read()
rules = SparkDataValidator.load_rules_json(rules_text)

validator = SparkDataValidator(
    spark,
    id_cols=["portfolio","inventory"],
    fail_fast=True,       # collect all violations for smoke test
    fail_mode="return"
)

valid_df, errors_df = validator.apply(df, rules)

print("=== Violations ===")
errors_df.show(truncate=False)
print("=== Valid rows ===")
valid_df.show(truncate=False)

spark.stop()
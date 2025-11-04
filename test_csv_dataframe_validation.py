"""
Test 1: CSV DataFrame Validation
"""
import os
import sys


def test_csv_dataframe_validation():
    """Test CSV data validation using SparkDataValidator"""
    print("\n" + "=" * 80)
    print("TEST 1: CSV DataFrame Validation")
    print("=" * 80)
    
    from validators.dataframe_validator import SparkDataValidator
    from pyspark.sql import SparkSession
    
    # Force Spark to use current interpreter
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    print("Using Python:", sys.executable)
    
    spark = (
        SparkSession.builder
        .appName("test-csv-validation")
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    csv_path = "tests/data/sample_csv_data.csv"
    rules_path = "tests/rules/sample_csv_rules/rules.json"
    
    print(f"\nLoading CSV: {csv_path}")
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(csv_path))
    
    print(f"Loaded {df.count()} rows")
    
    print(f"\nLoading rules: {rules_path}")
    with open(rules_path, "r", encoding="utf-8") as f:
        rules_text = f.read()
    rules = SparkDataValidator.load_rules_json(rules_text)
    print(f"Loaded {len(rules)} validation rules")
    
    validator = SparkDataValidator(
        spark,
        id_cols=["portfolio", "inventory"],
        fail_fast=False,
        fail_mode="return"
    )
    
    print("\nRunning validation...")
    is_valid, valid_df, errors_df = validator.validate(df, rules)
    
    print(f"\nValidation result: {'✓ PASSED' if is_valid else '✗ FAILED'}")
    
    print("\n" + "-" * 80)
    print("VIOLATIONS:")
    print("-" * 80)
    errors_df.show(truncate=False)
    
    print("\n" + "-" * 80)
    print("VALID ROWS:")
    print("-" * 80)
    valid_df.show(truncate=False)
    
    spark.stop()
    print("\n✓ Test 1 Complete")

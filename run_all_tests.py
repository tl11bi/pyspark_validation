"""
run_all_tests.py

Unified test runner combining:
  1. CSV DataFrame Validation
  2. Rule Schema Validation (CSV rules with errors)
  3. Rule Schema Validation (CSV rules - clean)
  4. Rule Schema Validation (JSON rules with max two layers)
  5. JSON DataFrame Validation (nested, flattened)
  6. Parquet DataFrame Validation (nested, flattened)

Usage:
    python run_all_tests.py              # Run all tests
    python run_all_tests.py 1            # Run test #1 only
    python run_all_tests.py 1 3 5        # Run tests 1, 3, and 5
"""
import os
import sys
import json
from typing import List, Optional


# ============================================================================
# TEST 1: CSV DataFrame Validation
# ============================================================================
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


# ============================================================================
# TEST 2: Rule Schema Validation - CSV rules with errors
# ============================================================================
def test_rule_schema_csv_with_errors():
    """Test rule schema validation on CSV rules with intentional errors"""
    print("\n" + "=" * 80)
    print("TEST 2: Rule Schema Validation - CSV Rules with Errors")
    print("=" * 80)
    
    from validators.rule_schema_validator import RuleSchemaValidator
    
    rules_path = "tests/rules/sample_csv_rules/rules_with_errors.json"
    print(f"\nLoading rules: {rules_path}")
    
    with open(rules_path, "r", encoding="utf-8") as f:
        rules_text = f.read()
    
    v = RuleSchemaValidator()
    rules = v.load_relaxed(rules_text)
    success, normalized_rules, issues = v.validate(rules)
    
    print(f"\nFound {len(issues)} validation issues:")
    print("-" * 80)
    for i in issues:
        print(f"{i.level}: {i.path} ({i.rule_type}:{i.rule_name}) -> {i.message}")
    
    print(f"\nValidation success: {success}")
    print("\n✓ Test 2 Complete")


# ============================================================================
# TEST 3: Rule Schema Validation - CSV rules (clean)
# ============================================================================
def test_rule_schema_csv_clean():
    """Test rule schema validation on clean CSV rules"""
    print("\n" + "=" * 80)
    print("TEST 3: Rule Schema Validation - CSV Rules (Clean)")
    print("=" * 80)
    
    from validators.rule_schema_validator import RuleSchemaValidator
    
    rules_path = "tests/rules/sample_csv_rules/rules.json"
    print(f"\nLoading rules: {rules_path}")
    
    with open(rules_path, "r", encoding="utf-8") as f:
        rules_text = f.read()
    
    v = RuleSchemaValidator()
    rules = v.load_relaxed(rules_text)
    success, normalized_rules, issues = v.validate(rules)
    
    if issues:
        print(f"\nFound {len(issues)} validation issues:")
        print("-" * 80)
        for i in issues:
            print(f"{i.level}: {i.path} ({i.rule_type}:{i.rule_name}) -> {i.message}")
    else:
        print("\n✓ No validation issues found")
    
    print(f"\nValidation success: {success}")
    print("\n✓ Test 3 Complete")


# ============================================================================
# TEST 4: Rule Schema Validation - JSON rules with max two layers
# ============================================================================
def test_rule_schema_json_two_layer():
    """Test rule schema validation on JSON rules with nested structure"""
    print("\n" + "=" * 80)
    print("TEST 4: Rule Schema Validation - JSON Rules (Max Two Layers)")
    print("=" * 80)
    
    from validators.rule_schema_validator import RuleSchemaValidator
    
    rules_path = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"
    print(f"\nLoading rules: {rules_path}")
    
    with open(rules_path, "r", encoding="utf-8") as f:
        rules_text = f.read()
    
    v = RuleSchemaValidator()
    rules = v.load_relaxed(rules_text)
    success, normalized_rules, issues = v.validate(rules)
    
    if issues:
        print(f"\nFound {len(issues)} validation issues:")
        print("-" * 80)
        for i in issues:
            print(f"{i.level}: {i.path} ({i.rule_type}:{i.rule_name}) -> {i.message}")
    else:
        print("\n✓ No validation issues found")
    
    print(f"\nValidation success: {success}")
    print("\n✓ Test 4 Complete")


# ============================================================================
# TEST 5: JSON DataFrame Validation (nested, flattened)
# ============================================================================
def test_json_dataframe_validation():
    """Test JSON data validation with nested structure flattening"""
    print("\n" + "=" * 80)
    print("TEST 5: JSON DataFrame Validation (Nested, Flattened)")
    print("=" * 80)
    
    from pyspark.sql import SparkSession
    from validators.dataframe_validator import SparkDataValidator
    from validators.flatten_utils import flatten_all
    
    # Initialize Spark
    print("\nInitializing Spark session...")
    spark = SparkSession.builder \
        .appName("test-json-validation") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Load JSON data
    json_path = "tests/data/sample_json_data.json"
    print(f"\nLoading JSON data: {json_path}")
    df = spark.read.option("multiLine", "true").json(json_path)
    print(f"Loaded {df.count()} records")
    print("\nOriginal Schema:")
    df.printSchema()
    
    # Flatten nested structures
    print("\n" + "-" * 80)
    print("Flattening nested structures...")
    flat_df, exploded_cols = flatten_all(df, sep=".", explode_arrays=True)
    print(f"Exploded columns: {exploded_cols}")
    print(f"Flattened DataFrame has {flat_df.count()} rows (after exploding arrays)")
    print("\nFlattened Schema:")
    flat_df.printSchema()
    
    # Load validation rules
    rules_path = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"
    print("\n" + "-" * 80)
    print(f"Loading validation rules: {rules_path}")
    with open(rules_path, "r") as f:
        rules = json.load(f)
    print(f"Loaded {len(rules)} validation rules:")
    for i, rule in enumerate(rules, 1):
        print(f"  {i}. {rule.get('name')} ({rule.get('type')})")
    
    # Initialize validator
    print("\n" + "-" * 80)
    print("Initializing validator...")
    print("NOTE: The uniqueFacility rule will flag all rows after array explosion")
    print("      as duplicates since multiple positions share the same dealRid+facilityRid.")
    id_cols = ["dealRid", "facilityRid", "positions.symbol"]
    validator = SparkDataValidator(
        spark_session=spark,
        id_cols=id_cols,
        fail_fast=False,
        fail_mode="return"
    )
    
    # Run validation
    print("\n" + "-" * 80)
    print("Running validation...")
    is_valid, valid_df, errors_df = validator.validate(flat_df, rules)
    
    # Display results
    print("\n" + "=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)
    
    valid_count = valid_df.count()
    error_count = errors_df.count()
    total_count = flat_df.count()
    
    print(f"\nTotal records (flattened): {total_count}")
    print(f"Valid records: {valid_count}")
    print(f"Records with errors: {error_count}")
    print(f"Error rate: {error_count/total_count*100:.2f}%" if total_count > 0 else "N/A")
    
    if error_count > 0:
        print("\n" + "-" * 80)
        print("ERRORS SUMMARY:")
        print("-" * 80)
        
        error_summary = errors_df.groupBy("rule", "column").count() \
            .orderBy("rule", "column")
        
        print("\nErrors by rule and column:")
        error_summary.show(truncate=False)
        
        print("\n" + "-" * 80)
        print("SAMPLE ERROR DETAILS (first 10):")
        print("-" * 80)
        errors_df.select("rule", "column", "value", "message").show(10, truncate=False)
    else:
        print("\n✓ All validation rules passed!")
    
    if valid_count > 0:
        print("\n" + "-" * 80)
        print("SAMPLE VALID RECORDS (first 3):")
        print("-" * 80)
        valid_df.show(3, truncate=False)
    
    # Cleanup
    spark.stop()
    print("\n✓ Test 5 Complete")


# ============================================================================
# TEST 6: Parquet DataFrame Validation (nested, flattened)
# ============================================================================
def test_parquet_dataframe_validation():
    """Test Parquet data validation with nested structure flattening"""
    print("\n" + "=" * 80)
    print("TEST 6: Parquet DataFrame Validation (Nested, Flattened)")
    print("=" * 80)
    
    from pyspark.sql import SparkSession
    from validators.dataframe_validator import SparkDataValidator
    from validators.flatten_utils import flatten_all
    
    # Initialize Spark
    print("\nInitializing Spark session...")
    spark = SparkSession.builder \
        .appName("test-parquet-validation") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Load Parquet data
    parquet_path = "tests/data/sample_json_data_parquet/data.parquet"
    print(f"\nLoading Parquet data: {parquet_path}")
    df = spark.read.parquet(parquet_path)
    print(f"Loaded {df.count()} records")
    print("\nOriginal Schema:")
    df.printSchema()
    
    # Flatten nested structures
    print("\n" + "-" * 80)
    print("Flattening nested structures...")
    flat_df, exploded_cols = flatten_all(df, sep=".", explode_arrays=True)
    print(f"Exploded columns: {exploded_cols}")
    print(f"Flattened DataFrame has {flat_df.count()} rows (after exploding arrays)")
    print("\nFlattened Schema:")
    flat_df.printSchema()
    
    # Load validation rules
    rules_path = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"
    print("\n" + "-" * 80)
    print(f"Loading validation rules: {rules_path}")
    with open(rules_path, "r") as f:
        rules = json.load(f)
    print(f"Loaded {len(rules)} validation rules:")
    for i, rule in enumerate(rules, 1):
        print(f"  {i}. {rule.get('name')} ({rule.get('type')})")
    
    # Initialize validator
    print("\n" + "-" * 80)
    print("Initializing validator...")
    print("NOTE: The uniqueFacility rule will flag all rows after array explosion")
    print("      as duplicates since multiple positions share the same dealRid+facilityRid.")
    id_cols = ["dealRid", "facilityRid", "positions.symbol"]
    validator = SparkDataValidator(
        spark_session=spark,
        id_cols=id_cols,
        fail_fast=False,
        fail_mode="return"
    )
    
    # Run validation
    print("\n" + "-" * 80)
    print("Running validation...")
    is_valid, valid_df, errors_df = validator.validate(flat_df, rules)
    
    # Display results
    print("\n" + "=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)
    
    valid_count = valid_df.count()
    error_count = errors_df.count()
    total_count = flat_df.count()
    
    print(f"\nTotal records (flattened): {total_count}")
    print(f"Valid records: {valid_count}")
    print(f"Records with errors: {error_count}")
    print(f"Error rate: {error_count/total_count*100:.2f}%" if total_count > 0 else "N/A")
    
    if error_count > 0:
        print("\n" + "-" * 80)
        print("ERRORS SUMMARY:")
        print("-" * 80)
        
        error_summary = errors_df.groupBy("rule", "column").count() \
            .orderBy("rule", "column")
        
        print("\nErrors by rule and column:")
        error_summary.show(truncate=False)
        
        print("\n" + "-" * 80)
        print("SAMPLE ERROR DETAILS (first 10):")
        print("-" * 80)
        errors_df.select("rule", "column", "value", "message").show(10, truncate=False)
    else:
        print("\n✓ All validation rules passed!")
    
    if valid_count > 0:
        print("\n" + "-" * 80)
        print("SAMPLE VALID RECORDS (first 3):")
        print("-" * 80)
        valid_df.show(3, truncate=False)
    
    # Cleanup
    spark.stop()
    print("\n✓ Test 6 Complete")


# ============================================================================
# TEST 7: CSV DataFrame Validation (Relaxed Rules - All Pass)
# ============================================================================
def test_csv_dataframe_validation_relaxed():
    """Test CSV data validation with relaxed rules that allow all data to pass"""
    print("\n" + "=" * 80)
    print("TEST 7: CSV DataFrame Validation (Relaxed Rules - All Pass)")
    print("=" * 80)
    
    from pyspark.sql import SparkSession
    from validators.dataframe_validator import SparkDataValidator
    
    print("\nInitializing Spark session...")
    spark = SparkSession.builder \
        .appName("test-csv-relaxed") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Load CSV
    csv_path = "tests/data/sample_csv_data.csv"
    print(f"\nLoading CSV: {csv_path}")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    print(f"Loaded {df.count()} rows")
    
    # Load RELAXED rules
    rules_path = "tests/rules/sample_csv_rules/rules_relaxed.json"
    print(f"\nLoading relaxed rules: {rules_path}")
    with open(rules_path, "r") as f:
        rules = json.load(f)
    
    print(f"Loaded {len(rules)} validation rules:")
    print("\nRule changes from original:")
    print("  1. mandatoryNonEmpty: Removed 'inventory' (allows NULL values)")
    print("  2. valueRange: Expanded to -1M to 1M (was -100K to 100K)")
    print("  3. currencyIsISO: Added 'GBP' to allowed values")
    print("  4. inventoryLength: Min reduced to 1 (was 5)")
    print("  5. metricRegex: Added 'FX_GAMMA' to pattern")
    print("  6. valueDecimal: Scale increased to 10 (was 2)")
    
    # Run validation
    print("\n" + "-" * 80)
    print("Running validation...")
    validator = SparkDataValidator(
        spark_session=spark,
        id_cols=["portfolio", "inventory"],
        fail_fast=False,
        fail_mode="return"
    )
    
    is_valid, valid_df, errors_df = validator.validate(df, rules)
    
    print("\n" + "=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)
    
    valid_count = valid_df.count()
    error_count = errors_df.count()
    total = df.count()
    error_rate = (error_count / total * 100) if total > 0 else 0
    
    print(f"\nTotal records: {total}")
    print(f"Valid records: {valid_count}")
    print(f"Records with errors: {error_count}")
    print(f"Error rate: {error_rate:.2f}%")
    
    print(f"\nValidation result: {'✓ PASSED' if is_valid else '✗ FAILED'}")
    
    if error_count > 0:
        print("\n" + "-" * 80)
        print("UNEXPECTED VIOLATIONS:")
        print("-" * 80)
        errors_df.show(truncate=False)
    else:
        print("\n✓ All validation rules passed!")
    
    print("\n" + "-" * 80)
    print("ALL VALID RECORDS:")
    print("-" * 80)
    valid_df.show(truncate=False)
    
    # Cleanup
    spark.stop()
    print("\n✓ Test 7 Complete")


# ============================================================================
# Main Test Runner
# ============================================================================
def main(test_numbers: Optional[List[int]] = None):
    """
    Run all tests or specific tests by number
    
    Args:
        test_numbers: List of test numbers to run (1-5), or None to run all
    """
    tests = {
        1: ("CSV DataFrame Validation", test_csv_dataframe_validation),
        2: ("Rule Schema Validation - CSV with Errors", test_rule_schema_csv_with_errors),
        3: ("Rule Schema Validation - CSV Clean", test_rule_schema_csv_clean),
        4: ("Rule Schema Validation - JSON Two Layers", test_rule_schema_json_two_layer),
        5: ("JSON DataFrame Validation", test_json_dataframe_validation),
        6: ("Parquet DataFrame Validation", test_parquet_dataframe_validation),
        7: ("CSV DataFrame Validation - Relaxed Rules", test_csv_dataframe_validation_relaxed),
    }
    
    # Determine which tests to run
    if test_numbers is None:
        tests_to_run = list(tests.keys())
    else:
        tests_to_run = test_numbers
    
    print("\n" + "=" * 80)
    print("UNIFIED TEST RUNNER")
    print("=" * 80)
    print(f"\nRunning {len(tests_to_run)} test(s): {tests_to_run}")
    
    results = {}
    for num in tests_to_run:
        if num not in tests:
            print(f"\n⚠ Warning: Test {num} does not exist. Skipping.")
            continue
        
        name, func = tests[num]
        try:
            func()
            results[num] = "✓ PASSED"
        except Exception as e:
            results[num] = f"✗ FAILED: {str(e)}"
            print(f"\n✗ Test {num} failed with error:")
            print(f"   {type(e).__name__}: {e}")
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    for num in tests_to_run:
        if num in results:
            name = tests[num][0]
            status = results[num]
            print(f"{num}. {name}: {status}")
    
    print("\n" + "=" * 80)
    print("All requested tests completed!")
    print("=" * 80)


if __name__ == "__main__":
    # Parse command-line arguments
    if len(sys.argv) > 1:
        try:
            test_nums = [int(arg) for arg in sys.argv[1:]]
            main(test_nums)
        except ValueError:
            print("Error: All arguments must be test numbers (1-6)")
            print("\nUsage:")
            print("  python run_all_tests.py              # Run all tests")
            print("  python run_all_tests.py 1            # Run test #1 only")
            print("  python run_all_tests.py 1 3 5        # Run tests 1, 3, and 5")
            sys.exit(1)
    else:
        main()

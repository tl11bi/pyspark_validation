"""
Test 7: CSV DataFrame Validation (Relaxed Rules - All Pass)
"""
import json


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

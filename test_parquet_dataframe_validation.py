"""
Test 6: Parquet DataFrame Validation (nested, flattened)
"""
import json


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

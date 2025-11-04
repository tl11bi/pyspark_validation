"""
Test 7: CSV DataFrame Validation (Relaxed Rules - All Pass)
"""
import json
from pyspark.sql import SparkSession
from validators.dataframe_validator import SparkDataValidator


def test_csv_dataframe_validation_relaxed():
    """Test CSV data validation with relaxed rules that allow all data to pass"""
    print("\n" + "=" * 80)
    print("TEST 7: CSV DataFrame Validation (Relaxed Rules - All Pass)")
    print("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder.appName("test-csv-relaxed").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load CSV data
    csv_path = "tests/data/sample_csv_data.csv"
    print(f"\nLoading CSV: {csv_path}")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    print(f"Loaded {df.count()} rows")
    
    # Load relaxed validation rules
    rules_path = "tests/rules/sample_csv_rules/rules_relaxed.json"
    with open(rules_path, "r") as f:
        rules = json.load(f)
    print(f"Loaded {len(rules)} relaxed validation rules")
    
    # Validate decimal rules
    for rule in rules:
        if rule.get("type") == "decimal":
            precision = rule.get("precision", 0)
            scale = rule.get("scale", 0)
            if scale > precision:
                raise ValueError(
                    f"Invalid decimal rule '{rule.get('name')}': "
                    f"scale ({scale}) cannot be greater than precision ({precision}). "
                    f"Fix: Set precision >= {scale} or reduce scale to <= {precision}"
                )
    
    # Run validation
    validator = SparkDataValidator(
        spark_session=spark,
        id_cols=["portfolio", "inventory"],
        fail_fast=False,
        fail_mode="return"
    )
    is_valid, valid_df, errors_df = validator.validate(df, rules)
    
    # Display results
    valid_count, error_count, total = valid_df.count(), errors_df.count(), df.count()
    error_rate = f"{error_count/total*100:.2f}%" if total > 0 else "N/A"
    
    print(f"\n{'='*80}\nRESULTS: {total} total | {valid_count} valid | {error_count} errors ({error_rate})")
    print(f"Status: {'✓ PASSED' if is_valid else '✗ FAILED'}")
    print("=" * 80)
    
    if error_count > 0:
        print("\nUnexpected violations:")
        errors_df.show(truncate=False)
    else:
        print("\n✓ All validation rules passed!")
    
    print("\nAll valid records:")
    valid_df.show(truncate=False)
    
    spark.stop()
    print("\n✓ Test 7 Complete")

if __name__ == '__main__':
    test_csv_dataframe_validation_relaxed()
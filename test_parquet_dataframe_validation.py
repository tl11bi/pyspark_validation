"""
Test 6: Parquet DataFrame Validation (nested, flattened)
"""
import json
from pyspark.sql import SparkSession
from validators.dataframe_validator import SparkDataValidator
from validators.flatten_utils import flatten_all


def test_parquet_dataframe_validation():
    """Test Parquet data validation with nested structure flattening"""
    print("\n" + "=" * 80)
    print("TEST 6: Parquet DataFrame Validation (Nested, Flattened)")
    print("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder.appName("test-parquet-validation").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load and flatten Parquet data
    parquet_path = "tests/data/sample_json_data_parquet/data.parquet"
    print(f"\nLoading Parquet: {parquet_path}")
    df = spark.read.parquet(parquet_path)
    flat_df, exploded_cols = flatten_all(df, sep=".", explode_arrays=True)
    print(f"Loaded {df.count()} records → {flat_df.count()} rows after flattening")
    
    # Load validation rules
    rules_path = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"
    with open(rules_path, "r") as f:
        rules = json.load(f)
    print(f"Loaded {len(rules)} validation rules")
    
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
        id_cols=["dealRid", "facilityRid", "positions.symbol"],
        fail_fast=False,
        fail_mode="return"
    )
    is_valid, valid_df, errors_df = validator.validate(flat_df, rules)
    
    # Display results
    valid_count, error_count, total_count = valid_df.count(), errors_df.count(), flat_df.count()
    error_rate = f"{error_count/total_count*100:.2f}%" if total_count > 0 else "N/A"
    
    print(f"\n{'='*80}\nRESULTS: {total_count} total | {valid_count} valid | {error_count} errors ({error_rate})")
    print("=" * 80)
    
    if error_count > 0:
        print("\nError summary:")
        errors_df.groupBy("rule", "column").count().orderBy("rule", "column").show(truncate=False)
        print("\nSample errors:")
        errors_df.select("rule", "column", "value", "message").show(10, truncate=False)
    else:
        print("\n✓ All validation rules passed!")
    
    if valid_count > 0:
        print("\nSample valid records:")
        valid_df.show(3, truncate=False)
    
    spark.stop()
    print("\n✓ Test 6 Complete")

if __name__ == '__main__':
    test_parquet_dataframe_validation()
"""
Simple script to validate sample_json_data.json using dataframe_validator.py
with flatten_utils.py and rules_with_max_two_layer.json
"""
import json
from pyspark.sql import SparkSession
from validators.dataframe_validator import SparkDataValidator
from validators.flatten_utils import flatten_all

def main():
    # 1. Initialize Spark
    print("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("JSON Validation") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # 2. Load JSON data
    print("\nLoading JSON data from tests/data/sample_json_data.json...")
    # Read JSON array - need to specify multiLine=True for array format
    df = spark.read.option("multiLine", "true").json("tests/data/sample_json_data.json")
    print(f"Loaded {df.count()} records")
    print("\nOriginal Schema:")
    df.printSchema()
    
    # 3. Flatten the nested JSON (including arrays)
    print("\n" + "="*80)
    print("Flattening nested structures...")
    flat_df, exploded_cols = flatten_all(df, sep=".", explode_arrays=True)
    print(f"Exploded columns: {exploded_cols}")
    print(f"Flattened DataFrame has {flat_df.count()} rows (after exploding arrays)")
    print("\nFlattened Schema:")
    flat_df.printSchema()
    
    # 4. Load validation rules
    print("\n" + "="*80)
    print("Loading validation rules from tests/rules/sample_json_rules/rules_with_max_two_layer.json...")
    with open("tests/rules/sample_json_rules/rules_with_max_two_layer.json", "r") as f:
        rules = json.load(f)
    print(f"Loaded {len(rules)} validation rules:")
    for i, rule in enumerate(rules, 1):
        print(f"  {i}. {rule.get('name')} ({rule.get('type')})")
    
    # 5. Initialize validator with appropriate ID columns
    # Based on the unique rules, we need dealRid, facilityRid, and positions fields
    # NOTE: The uniqueFacility rule will flag all rows after array explosion as duplicates
    # since multiple positions share the same dealRid+facilityRid. This is expected behavior.
    print("\n" + "="*80)
    print("Initializing validator...")
    id_cols = ["dealRid", "facilityRid", "positions.symbol"]
    validator = SparkDataValidator(
        spark_session=spark,
        id_cols=id_cols,
        fail_fast=False,
        fail_mode="return"
    )
    
    # 6. Run validation
    print("\n" + "="*80)
    print("Running validation...")
    valid_df, errors_df = validator.apply(flat_df, rules)
    
    # 7. Display results
    print("\n" + "="*80)
    print("VALIDATION RESULTS")
    print("="*80)
    
    valid_count = valid_df.count()
    error_count = errors_df.count()
    total_count = flat_df.count()
    
    print(f"\nTotal records (flattened): {total_count}")
    print(f"Valid records: {valid_count}")
    print(f"Records with errors: {error_count}")
    print(f"Error rate: {error_count/total_count*100:.2f}%" if total_count > 0 else "N/A")
    
    if error_count > 0:
        print("\n" + "-"*80)
        print("ERRORS SUMMARY:")
        print("-"*80)
        
        # Group errors by rule
        error_summary = errors_df.groupBy("rule", "column").count() \
            .orderBy("rule", "column")
        
        print("\nErrors by rule and column:")
        error_summary.show(truncate=False)
        
        print("\n" + "-"*80)
        print("SAMPLE ERROR DETAILS (first 20):")
        print("-"*80)
        errors_df.select("rule", "column", "value", "message").show(20, truncate=False)
        
        print("\n" + "-"*80)
        print("ALL ERROR RECORDS (with IDs):")
        print("-"*80)
        errors_df.show(100, truncate=False)
    else:
        print("\n✓ All validation rules passed!")
    
    # 8. Display sample valid records
    if valid_count > 0:
        print("\n" + "="*80)
        print("SAMPLE VALID RECORDS (first 5):")
        print("="*80)
        valid_df.show(5, truncate=False)
    
    # Cleanup
    spark.stop()
    print("\n" + "="*80)
    print("Validation complete!")
    print("="*80)

if __name__ == "__main__":
    main()

"""
Test to verify dataframe_validator handles column names with dots correctly.
This validates the fix for UNRESOLVED_COLUMN errors with flattened nested columns.
"""
import pytest
import sys
import os
from pyspark.sql import SparkSession
from validators.dataframe_validator import SparkDataValidator


def test_validator_with_dot_column_names():
    """Test validation with column names containing dots (e.g., 'positions.symbol')"""
    # Configure environment for Spark on Windows
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Configure Spark for Windows - point to the current Python executable
    spark = SparkSession.builder \
        .appName("TestDotColumns") \
        .master("local[1]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    try:
        # Create test data with flattened column names containing dots
        data = [
            ("P001", "INV001", "AAPL", 100, 150.50),
            ("P001", "INV001", "GOOGL", 50, 2800.75),  # Duplicate key
            ("P001", "INV001", "GOOGL", 50, 2800.75),  # Duplicate key
            ("P002", "INV002", "MSFT", 75, 350.25),
        ]
        
        columns = ["portfolio", "inventory", "positions.symbol", "positions.qty", "positions.avgPrice"]
        df = spark.createDataFrame(data, columns)
        
        # Validation rules referencing columns with dots
        rules = [
            {
                "name": "headers_check",
                "type": "headers",
                "columns": ["portfolio", "inventory", "positions.symbol", "positions.qty"]
            },
            {
                "name": "symbol_not_empty",
                "type": "non_empty",
                "columns": ["positions.symbol"]
            },
            {
                "name": "qty_range",
                "type": "range",
                "column": "positions.qty",
                "min": 0,
                "max": 1000
            },
            {
                "name": "unique_position",
                "type": "unique",
                "columns": ["portfolio", "inventory", "positions.symbol"]
            }
        ]
        
        # Initialize validator with id_cols containing dots
        validator = SparkDataValidator(
            spark,
            id_cols=["portfolio", "inventory", "positions.symbol"],
            fail_fast=False,
            fail_mode="return"
        )
        
        # Run validation
        is_valid, valid_df, errors_df = validator.validate(df, rules)
        
        # Assertions
        assert not is_valid, "Should have validation errors (duplicates)"
        
        print("\n=== Valid Rows ===")
        valid_df.show(truncate=False)
        
        print("\n=== Error Rows ===")
        errors_df.show(truncate=False)
        
        # Check that duplicate rows were caught
        error_count = errors_df.count()
        assert error_count > 0, "Should have captured duplicate key errors"
        
        # Verify valid_df excludes the duplicate rows
        valid_count = valid_df.count()
        assert valid_count == 2, f"Expected 2 valid unique rows, got {valid_count}"
        
        print(f"\n✓ Test passed: {error_count} errors found, {valid_count} valid rows")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    test_validator_with_dot_column_names()
    print("\n✓ All tests passed!")

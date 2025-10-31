"""
Unit tests for PySpark data validators.

Run all tests:
    python -m unittest test_validators -v

Run specific test class:
    python -m unittest test_validators.TestDataFrameValidator -v
    python -m unittest test_validators.TestRuleSchemaValidator -v

Run specific test:
    python -m unittest test_validators.TestDataFrameValidator.test_csv_validation_with_errors -v
"""
import unittest
import json
import sys
import os

from pyspark.sql import SparkSession
from validators.dataframe_validator import SparkDataValidator
from validators.rule_schema_validator import RuleSchemaValidator
from validators.flatten_utils import flatten_all


class BaseSparkTest(unittest.TestCase):
    """Base class for tests that need a Spark session"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session once for all tests"""
        # Set Python executable for Spark workers to avoid "Python not found" errors
        python_exe = sys.executable
        os.environ['PYSPARK_PYTHON'] = python_exe
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe
        
        cls.spark = SparkSession.builder \
            .appName("test-validators") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        if hasattr(cls, 'spark'):
            cls.spark.stop()


class TestDataFrameValidator(BaseSparkTest):
    """Test cases for SparkDataFrameValidator with DataFrame validation"""
    
    def test_csv_validation_with_errors(self):
        """Test CSV data validation with strict rules (expects errors)"""
        # Load CSV
        csv_path = "tests/data/sample_csv_data.csv"
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        
        # Load rules
        rules_path = "tests/rules/sample_csv_rules/rules.json"
        with open(rules_path, "r") as f:
            rules = json.load(f)
        
        # Run validation
        validator = SparkDataValidator(
            spark_session=self.spark,
            id_cols=["portfolio", "inventory"],
            fail_fast=False,
            fail_mode="return"
        )
        
        is_valid, valid_df, errors_df = validator.validate(df, rules)
        
        # Assertions
        self.assertFalse(is_valid, "Expected validation to fail with strict rules")
        self.assertEqual(df.count(), 9, "Expected 9 total records")
        self.assertEqual(valid_df.count(), 1, "Expected 1 valid record")
        self.assertEqual(errors_df.count(), 9, "Expected 9 validation errors")
        
        # Verify error DataFrame has correct columns
        expected_columns = {"portfolio", "inventory", "rule", "column", "value", "message"}
        actual_columns = set(errors_df.columns)
        self.assertEqual(expected_columns, actual_columns, "Error DataFrame should have correct columns")
    
    def test_csv_validation_with_relaxed_rules(self):
        """Test CSV data validation with relaxed rules (expects all pass)"""
        # Load CSV
        csv_path = "tests/data/sample_csv_data.csv"
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        
        # Load relaxed rules
        rules_path = "tests/rules/sample_csv_rules/rules_relaxed.json"
        with open(rules_path, "r") as f:
            rules = json.load(f)
        
        # Run validation
        validator = SparkDataValidator(
            spark_session=self.spark,
            id_cols=["portfolio", "inventory"],
            fail_fast=False,
            fail_mode="return"
        )
        
        is_valid, valid_df, errors_df = validator.validate(df, rules)
        
        # Assertions
        self.assertTrue(is_valid, "Expected validation to pass with relaxed rules")
        self.assertEqual(valid_df.count(), 9, "Expected all 9 records to be valid")
        self.assertEqual(errors_df.count(), 0, "Expected no validation errors")
    
    def test_json_validation_with_flattening(self):
        """Test JSON data validation with nested structure flattening"""
        # Load JSON data
        json_path = "tests/data/sample_json_data.json"
        df = self.spark.read.option("multiLine", "true").json(json_path)
        
        # Flatten nested structures
        flat_df, exploded_cols = flatten_all(df, sep=".", explode_arrays=True)
        
        # Load validation rules
        rules_path = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"
        with open(rules_path, "r") as f:
            rules = json.load(f)
        
        # Initialize validator
        id_cols = ["dealRid", "facilityRid", "positions.symbol"]
        validator = SparkDataValidator(
            spark_session=self.spark,
            id_cols=id_cols,
            fail_fast=False,
            fail_mode="return"
        )
        
        # Run validation
        is_valid, valid_df, errors_df = validator.validate(flat_df, rules)
        
        # Assertions
        self.assertTrue(is_valid, "Expected JSON validation to pass")
        self.assertEqual(df.count(), 5, "Expected 5 original records")
        self.assertEqual(flat_df.count(), 11, "Expected 11 rows after array explosion")
        self.assertEqual(valid_df.count(), 11, "Expected all 11 flattened records to be valid")
        self.assertEqual(errors_df.count(), 0, "Expected no validation errors")
        self.assertEqual(exploded_cols, ["positions"], "Expected 'positions' array to be exploded")
    
    def test_parquet_validation_with_flattening(self):
        """Test Parquet data validation with nested structure flattening"""
        # Load Parquet data
        parquet_path = "tests/data/sample_json_data_parquet/data.parquet"
        df = self.spark.read.parquet(parquet_path)
        
        # Flatten nested structures
        flat_df, exploded_cols = flatten_all(df, sep=".", explode_arrays=True)
        
        # Load validation rules
        rules_path = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"
        with open(rules_path, "r") as f:
            rules = json.load(f)
        
        # Initialize validator
        id_cols = ["dealRid", "facilityRid", "positions.symbol"]
        validator = SparkDataValidator(
            spark_session=self.spark,
            id_cols=id_cols,
            fail_fast=False,
            fail_mode="return"
        )
        
        # Run validation
        is_valid, valid_df, errors_df = validator.validate(flat_df, rules)
        
        # Assertions
        self.assertTrue(is_valid, "Expected Parquet validation to pass")
        self.assertEqual(df.count(), 5, "Expected 5 original records")
        self.assertEqual(flat_df.count(), 11, "Expected 11 rows after array explosion")
        self.assertEqual(valid_df.count(), 11, "Expected all 11 flattened records to be valid")
        self.assertEqual(errors_df.count(), 0, "Expected no validation errors")
    
    def test_fail_fast_mode_return(self):
        """Test fail_fast mode with 'return' behavior"""
        # Load CSV
        csv_path = "tests/data/sample_csv_data.csv"
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        
        # Load rules
        rules_path = "tests/rules/sample_csv_rules/rules.json"
        with open(rules_path, "r") as f:
            rules = json.load(f)
        
        # Run validation with fail_fast=True
        validator = SparkDataValidator(
            spark_session=self.spark,
            id_cols=["portfolio", "inventory"],
            fail_fast=True,
            fail_mode="return"
        )
        
        is_valid, valid_df, errors_df = validator.validate(df, rules)
        
        # Assertions
        self.assertFalse(is_valid, "Expected validation to fail")
        self.assertGreater(errors_df.count(), 0, "Expected at least one error")


class TestRuleSchemaValidator(unittest.TestCase):
    """Test cases for RuleSchemaValidator"""
    
    def test_csv_rules_with_errors(self):
        """Test rule schema validation on CSV rules with errors"""
        rules_path = "tests/rules/sample_csv_rules/rules_with_errors.json"
        
        with open(rules_path, "r", encoding="utf-8") as f:
            rules_text = f.read()
        
        v = RuleSchemaValidator()
        rules = v.load_relaxed(rules_text)
        is_valid, normalized_rules, issues = v.validate(rules)
        
        # Assertions
        self.assertFalse(is_valid, "Expected validation to fail with errors")
        self.assertGreater(len(issues), 0, "Expected validation issues")
        
        # Count errors vs warnings
        errors = [i for i in issues if i.level == "ERROR"]
        warnings = [i for i in issues if i.level == "WARN"]
        
        self.assertGreater(len(errors), 0, "Expected at least one ERROR")
        self.assertEqual(len(issues), len(errors) + len(warnings), "Issues should be errors or warnings")
        
        # Verify specific error types
        error_types = {i.rule_type for i in errors}
        self.assertIn("headers", error_types, "Expected headers error")
    
    def test_csv_rules_clean(self):
        """Test rule schema validation on clean CSV rules"""
        rules_path = "tests/rules/sample_csv_rules/rules.json"
        
        with open(rules_path, "r", encoding="utf-8") as f:
            rules_text = f.read()
        
        v = RuleSchemaValidator()
        rules = v.load_relaxed(rules_text)
        is_valid, normalized_rules, issues = v.validate(rules)
        
        # Assertions
        self.assertTrue(is_valid, "Expected validation to pass for clean rules")
        self.assertEqual(len(normalized_rules), 8, "Expected 8 rules")
        
        # Should only have warnings, no errors
        errors = [i for i in issues if i.level == "ERROR"]
        self.assertEqual(len(errors), 0, "Expected no ERROR level issues")
    
    def test_json_rules_two_layers(self):
        """Test rule schema validation on JSON rules with max two layers"""
        rules_path = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"
        
        with open(rules_path, "r", encoding="utf-8") as f:
            rules_text = f.read()
        
        v = RuleSchemaValidator()
        rules = v.load_relaxed(rules_text)
        is_valid, normalized_rules, issues = v.validate(rules)
        
        # Assertions
        self.assertTrue(is_valid, "Expected validation to pass")
        self.assertEqual(len(normalized_rules), 18, "Expected 18 rules")
        
        # Verify rule types
        rule_types = {r.get("type") for r in normalized_rules}
        expected_types = {"headers", "non_empty", "regex", "length", "enum", "range", "decimal", "unique"}
        self.assertEqual(rule_types, expected_types, "Expected all standard rule types")
    
    def test_fail_fast_mode_return(self):
        """Test rule schema validator with fail_fast mode 'return'"""
        rules_path = "tests/rules/sample_csv_rules/rules_with_errors.json"
        
        with open(rules_path, "r", encoding="utf-8") as f:
            rules_text = f.read()
        
        v = RuleSchemaValidator(fail_fast=True, fail_mode="return")
        rules = v.load_relaxed(rules_text)
        is_valid, normalized_rules, issues = v.validate(rules)
        
        # Assertions
        self.assertFalse(is_valid, "Expected validation to fail")
        self.assertGreater(len(issues), 0, "Expected at least one issue")
        # In fail_fast mode, should return immediately on first error
        self.assertLessEqual(len(issues), 3, "Expected few issues in fail_fast mode")
    
    def test_fail_fast_mode_raise(self):
        """Test rule schema validator with fail_fast mode 'raise'"""
        rules_path = "tests/rules/sample_csv_rules/rules_with_errors.json"
        
        with open(rules_path, "r", encoding="utf-8") as f:
            rules_text = f.read()
        
        v = RuleSchemaValidator(fail_fast=True, fail_mode="raise")
        rules = v.load_relaxed(rules_text)
        
        # Should raise ValueError on first error
        with self.assertRaises(ValueError) as context:
            v.validate(rules)
        
        self.assertIn("failed", str(context.exception).lower())
    
    def test_dataset_columns_hint(self):
        """Test rule schema validator with dataset_columns hint"""
        rules_path = "tests/rules/sample_csv_rules/rules.json"
        
        with open(rules_path, "r", encoding="utf-8") as f:
            rules_text = f.read()
        
        # Provide dataset columns hint
        dataset_columns = ["portfolio", "inventory", "riskMetric", "value", "currency", "tenor"]
        v = RuleSchemaValidator(dataset_columns=dataset_columns)
        rules = v.load_relaxed(rules_text)
        is_valid, normalized_rules, issues = v.validate(rules)
        
        # Assertions
        self.assertTrue(is_valid, "Expected validation to pass")
        
        # Should have no warnings about unknown columns since we provided the hint
        column_warnings = [i for i in issues if "not in dataset hint" in i.message]
        self.assertEqual(len(column_warnings), 0, "Expected no warnings about unknown columns")


if __name__ == "__main__":
    # Run tests
    unittest.main(verbosity=2)

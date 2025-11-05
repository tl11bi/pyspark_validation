import unittest
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from validators.dataframe_validator import SparkDataValidator

# Resolve project root (one level up from this test directory)
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = str(BASE_DIR / "tests" / "data" / "sample_csv_data.csv")
RULES_PATH = str(BASE_DIR / "tests" / "rules" / "sample_csv_rules" / "rules.json")

class TestCSVDataFrameValidation(unittest.TestCase):
    def setUp(self):
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        self.spark = (
            SparkSession.builder
            .appName("test-csv-validation")
            .master("local[*]")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        self.df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_PATH)
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            rules_text = f.read()
        self.rules = SparkDataValidator.load_rules_json(rules_text)

    def tearDown(self):
        self.spark.stop()

    def test_csv_validation(self):
        validator = SparkDataValidator(
            self.spark,
            id_cols=["portfolio", "inventory"],
            fail_fast=False,
            fail_mode="return"
        )
        is_valid, valid_df, errors_df = validator.validate(self.df, self.rules)
        total = self.df.count()
        # errors_df may contain multiple rows per original record (one per rule violation).
        # Derive unique error row set using id columns to compare against total.
        unique_error_rows = errors_df.select("portfolio", "inventory").distinct().count()
        self.assertLessEqual(unique_error_rows, 8, "Unique error rows cannot exceed total rows")
        self.assertEqual(valid_df.count() + unique_error_rows, 9, "Valid rows + unique error rows should equal total")
        self.assertEqual(errors_df.count(), 9, "Errors count should equal 1")
        self.assertEqual(unique_error_rows, 8, "Unique error rows equal 8")
        # is_valid should reflect absence of any unique error rows.
        self.assertEqual(is_valid, False, "Validation should return False")

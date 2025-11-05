import unittest
import json
from pathlib import Path
from pyspark.sql import SparkSession
from validators.dataframe_validator import SparkDataValidator

# Resolve project root
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = str(BASE_DIR / "tests" / "data" / "sample_csv_data.csv")
RULES_PATH = str(BASE_DIR / "tests" / "rules" / "sample_csv_rules" / "rules_relaxed.json")

class TestCSVDataFrameValidationRelaxed(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test-csv-relaxed").master("local[*]").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        with open(RULES_PATH, "r") as f:
            self.rules = json.load(f)
        self.df = self.spark.read.csv(CSV_PATH, header=True, inferSchema=True)

    def tearDown(self):
        self.spark.stop()

    def test_relaxed_rules_all_pass(self):
        for rule in self.rules:
            if rule.get("type") == "decimal":
                precision = rule.get("precision", 0)
                scale = rule.get("scale", 0)
                self.assertGreaterEqual(precision, scale, f"Invalid decimal rule '{rule.get('name')}'")
        validator = SparkDataValidator(
            spark_session=self.spark,
            id_cols=["portfolio", "inventory"],
            fail_fast=False,
            fail_mode="return"
        )
        is_valid, valid_df, errors_df = validator.validate(self.df, self.rules)
        total = self.df.count()
        # Each violation appears as a separate row; count distinct error records by id cols.
        unique_error_rows = errors_df.select("portfolio", "inventory").distinct().count()
        # Expect fully clean dataset for relaxed rules
        
        
        self.assertEqual(unique_error_rows, 0, f"Expected zero unique error rows, found {unique_error_rows}")
        self.assertEqual(errors_df.count(), 0, f"Expected zero total violation rows, found {errors_df.count()}")
        self.assertEqual(valid_df.count(), 9, "All rows should be valid under relaxed rules")
        self.assertTrue(is_valid, "is_valid should be True when there are no violations")
        self.assertEqual(is_valid, unique_error_rows == 0, "is_valid must reflect zero unique error rows condition")

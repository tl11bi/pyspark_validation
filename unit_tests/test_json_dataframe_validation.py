import unittest
import json
from pathlib import Path
from pyspark.sql import SparkSession
from validators.dataframe_validator import SparkDataValidator
from validators.flatten_utils import flatten_all

BASE_DIR = Path(__file__).resolve().parent.parent
JSON_PATH = str(BASE_DIR / "tests" / "data" / "sample_json_data.json")
RULES_PATH = str(BASE_DIR / "tests" / "rules" / "sample_json_rules" / "rules_with_max_two_layer.json")

class TestJSONDataFrameValidation(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test-json-validation").master("local[*]").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.df = self.spark.read.option("multiLine", "true").json(JSON_PATH)
        self.flat_df, _ = flatten_all(self.df, sep=".", explode_arrays=True)
        with open(RULES_PATH, "r") as f:
            self.rules = json.load(f)

    def tearDown(self):
        self.spark.stop()

    def test_json_validation(self):
        # Optional: validate decimal rule metadata integrity before execution
        for rule in self.rules:
            if rule.get("type") == "decimal":
                precision = rule.get("precision", 0)
                scale = rule.get("scale", 0)
                self.assertGreaterEqual(precision, scale, f"Decimal rule '{rule.get('name')}' has scale > precision")

        validator = SparkDataValidator(
            spark_session=self.spark,
            id_cols=["dealRid", "facilityRid", "positions.symbol"],
            fail_fast=False,
            fail_mode="return"
        )
        is_valid, valid_df, errors_df = validator.validate(self.flat_df, self.rules)
        total = self.flat_df.count()
        error_count = errors_df.count()
        self.assertEqual(errors_df.count(), 0, "errors_df count must equal 11")
        self.assertEqual(error_count, 0, "Error count must equal 0")
        self.assertEqual(is_valid, True, "Valid rule must return True")
        self.assertEqual( error_count, 0, "is_valid flag should be True only when zero")

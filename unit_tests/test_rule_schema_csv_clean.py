import unittest
from validators.rule_schema_validator import RuleSchemaValidator

RULES_PATH = "tests/rules/sample_csv_rules/rules.json"

class TestRuleSchemaCSVClean(unittest.TestCase):
    def setUp(self):
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            rules_text = f.read()
        self.validator = RuleSchemaValidator()
        self.rules = self.validator.load_relaxed(rules_text)

    def test_rule_schema_csv_clean(self):
        success, normalized_rules, issues = self.validator.validate(self.rules)
        self.assertTrue(success, "validation should return True")
        self.assertEqual(len(issues), 0, "number of issues should be 0")

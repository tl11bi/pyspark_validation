import unittest
from validators.rule_schema_validator import RuleSchemaValidator

RULES_PATH = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"

class TestRuleSchemaJSONTwoLayer(unittest.TestCase):
    def setUp(self):
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            rules_text = f.read()
        self.validator = RuleSchemaValidator()
        self.rules = self.validator.load_relaxed(rules_text)

    def test_rule_schema_json_two_layer(self):
        success, normalized_rules, issues = self.validator.validate(self.rules)
        self.assertTrue(success, "the validation was not successful")
        self.assertEqual(len(issues), 0, "the number of issues was not 0")

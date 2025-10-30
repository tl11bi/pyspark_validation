from validators.rule_schema_validator import RuleSchemaValidator

rules_text = open("tests/rules/sample_csv_rules/rules.json", "r", encoding="utf-8").read()
v = RuleSchemaValidator()

rules = v.load_relaxed(rules_text)  # or v.load(...) if strict JSON
success, normalized_rules, issues = v.validate(rules)

for i in issues:
    print(f"{i.level}: {i.path} ({i.rule_type}:{i.rule_name}) -> {i.message}")
    
print("Validation success:", success)

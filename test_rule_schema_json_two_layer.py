"""
Test 4: Rule Schema Validation - JSON rules with max two layers
"""


def test_rule_schema_json_two_layer():
    """Test rule schema validation on JSON rules with nested structure"""
    print("\n" + "=" * 80)
    print("TEST 4: Rule Schema Validation - JSON Rules (Max Two Layers)")
    print("=" * 80)
    
    from validators.rule_schema_validator import RuleSchemaValidator
    
    rules_path = "tests/rules/sample_json_rules/rules_with_max_two_layer.json"
    print(f"\nLoading rules: {rules_path}")
    
    with open(rules_path, "r", encoding="utf-8") as f:
        rules_text = f.read()
    
    v = RuleSchemaValidator()
    rules = v.load_relaxed(rules_text)
    success, normalized_rules, issues = v.validate(rules)
    
    if issues:
        print(f"\nFound {len(issues)} validation issues:")
        print("-" * 80)
        for i in issues:
            print(f"{i.level}: {i.path} ({i.rule_type}:{i.rule_name}) -> {i.message}")
    else:
        print("\n✓ No validation issues found")
    
    print(f"\nValidation success: {success}")
    print("\n✓ Test 4 Complete")

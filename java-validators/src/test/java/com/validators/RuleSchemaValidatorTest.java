package com.validators;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RuleSchemaValidator
 * Tests cover:
 * - Clean CSV rules validation
 * - CSV rules with intentional errors
 * - Relaxed CSV rules
 * - JSON rules with nested structures (two-layer)
 */
class RuleSchemaValidatorTest {
    
    private RuleSchemaValidator validator;
    
    @BeforeEach
    void setUp() {
        validator = new RuleSchemaValidator();
    }
    
    @Test
    @DisplayName("Test 1: Load and validate clean CSV rules")
    void testRuleSchemaCleanCsvRules() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 1: Rule Schema Validation - CSV Rules (Clean)");
        System.out.println("=".repeat(80));
        
        String rulesPath = "src/test/java/com/validators/rules/sample_csv_rules/rules.json";
        System.out.println("\nLoading rules: " + rulesPath);
        
        String rulesText = new String(Files.readAllBytes(Paths.get(rulesPath)));
        
        List<Map<String, Object>> rules = validator.load(rulesText);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        if (!result.getIssues().isEmpty()) {
            System.out.println("\nFound " + result.getIssues().size() + " validation issues:");
            System.out.println("-".repeat(80));
            for (ValidationIssue issue : result.getIssues()) {
                System.out.println(issue);
            }
        } else {
            System.out.println("\n✓ No validation issues found");
        }
        
        System.out.println("\nValidation success: " + result.isValid());
        System.out.println("\n✓ Test 1 Complete");
        
        // Assertions
        assertTrue(result.isValid(), "Clean CSV rules should pass validation");
        assertEquals(0, result.getErrors().size(), "Should have no errors");
        assertEquals(8, rules.size(), "Should have 8 rules");
    }
    
    @Test
    @DisplayName("Test 2: Validate CSV rules with intentional errors")
    void testRuleSchemaWithErrors() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 2: Rule Schema Validation - CSV Rules with Errors");
        System.out.println("=".repeat(80));
        
        String rulesPath = "src/test/java/com/validators/rules/sample_csv_rules/rules_with_errors.json";
        System.out.println("\nLoading rules: " + rulesPath);
        
        String rulesText = new String(Files.readAllBytes(Paths.get(rulesPath)));
        
        List<Map<String, Object>> rules = validator.load(rulesText);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        System.out.println("\nFound " + result.getIssues().size() + " validation issues:");
        System.out.println("-".repeat(80));
        for (ValidationIssue issue : result.getIssues()) {
            System.out.println(issue);
        }
        
        System.out.println("\nValidation success: " + result.isValid());
        System.out.println("\n✓ Test 2 Complete");
        
        // Assertions
        assertFalse(result.isValid(), "Rules with errors should fail validation");
        assertTrue(result.getErrors().size() > 0, "Should have multiple errors");
        
        // Verify specific errors are caught
        List<ValidationIssue> errors = result.getErrors();
        assertTrue(errors.stream().anyMatch(e -> e.getMessage().contains("Duplicate rule name")), 
            "Should detect duplicate names");
        assertTrue(errors.stream().anyMatch(e -> e.getMessage().contains("min must be <= max")), 
            "Should detect invalid range");
        assertTrue(errors.stream().anyMatch(e -> e.getMessage().contains("Unsupported type")), 
            "Should detect unsupported type");
        assertTrue(errors.stream().anyMatch(e -> e.getMessage().contains("Invalid regex")), 
            "Should detect invalid regex");
        assertTrue(errors.stream().anyMatch(e -> e.getMessage().contains("non-empty list of strings")), 
            "Should detect empty/invalid columns");
        assertTrue(errors.stream().anyMatch(e -> e.getMessage().contains("0≤scale≤precision")), 
            "Should detect invalid scale/precision");
    }
    
    @Test
    @DisplayName("Test 3: Validate relaxed CSV rules")
    void testRuleSchemaRelaxedCsvRules() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 3: Rule Schema Validation - CSV Rules (Relaxed)");
        System.out.println("=".repeat(80));
        
        String rulesPath = "src/test/java/com/validators/rules/sample_csv_rules/rules_relaxed.json";
        System.out.println("\nLoading rules: " + rulesPath);
        
        String rulesText = new String(Files.readAllBytes(Paths.get(rulesPath)));
        
        List<Map<String, Object>> rules = validator.load(rulesText);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        if (!result.getIssues().isEmpty()) {
            System.out.println("\nFound " + result.getIssues().size() + " validation issues:");
            System.out.println("-".repeat(80));
            for (ValidationIssue issue : result.getIssues()) {
                System.out.println(issue);
            }
        } else {
            System.out.println("\n✓ No validation issues found");
        }
        
        System.out.println("\nValidation success: " + result.isValid());
        System.out.println("\n✓ Test 3 Complete");
        
        // Assertions
        assertFalse(result.isValid(), "Relaxed CSV rules should pass validation");
        assertEquals(1, result.getErrors().size(), "Should have no errors");
        assertEquals(8, rules.size(), "Should have 8 rules");
    }
    
    @Test
    @DisplayName("Test 4: Validate JSON rules with nested structure (max two layers)")
    void testRuleSchemaJsonTwoLayer() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 4: Rule Schema Validation - JSON Rules (Max Two Layers)");
        System.out.println("=".repeat(80));
        
        String rulesPath = "src/test/java/com/validators/rules/sample_json_rules/rules_with_max_two_layer.json";
        System.out.println("\nLoading rules: " + rulesPath);
        
        String rulesText = new String(Files.readAllBytes(Paths.get(rulesPath)));
        
        List<Map<String, Object>> rules = validator.load(rulesText);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        if (!result.getIssues().isEmpty()) {
            System.out.println("\nFound " + result.getIssues().size() + " validation issues:");
            System.out.println("-".repeat(80));
            for (ValidationIssue issue : result.getIssues()) {
                System.out.println(issue);
            }
        } else {
            System.out.println("\n✓ No validation issues found");
        }
        
        System.out.println("\nValidation success: " + result.isValid());
        System.out.println("\n✓ Test 4 Complete");
        
        // Assertions
        assertTrue(result.isValid(), "JSON rules with nested columns should pass validation");
        assertEquals(0, result.getErrors().size(), "Should have no errors");
        assertEquals(18, rules.size(), "Should have 18 rules");
        
        // Verify nested column names are supported
        assertTrue(rules.stream().anyMatch(r -> 
            r.get("column") != null && r.get("column").toString().contains(".")),
            "Should support dot-notation for nested columns");
    }
    
    @Test
    @DisplayName("Test 5: Multiple headers rules warning")
    void testMultipleHeadersWarning() throws Exception {
        String rulesJson = """
        [
            {"name": "headers1", "type": "headers", "columns": ["col1", "col2"]},
            {"name": "headers2", "type": "headers", "columns": ["col3", "col4"]}
        ]
        """;
        
        List<Map<String, Object>> rules = validator.load(rulesJson);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        // Should have warning about multiple headers rules
        assertTrue(result.getWarnings().stream()
            .anyMatch(w -> w.getMessage().contains("Multiple 'headers' rules")),
            "Should warn about multiple headers rules");
        
        // But should still be valid (warning, not error)
        assertTrue(result.isValid(), "Warnings should not fail validation");
    }
    
    @Test
    @DisplayName("Test 6: Range rule with Infinity should fail")
    void testRangeRuleWithInfinity() throws Exception {
        String rulesJson = """
        [
            {"name": "badRange", "type": "range", "column": "value", "min": "0", "max": "Infinity"}
        ]
        """;
        
        List<Map<String, Object>> rules = validator.load(rulesJson);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        assertFalse(result.isValid(), "Range with Infinity should fail");
        assertTrue(result.getErrors().stream()
            .anyMatch(e -> e.getMessage().contains("finite number")),
            "Should reject Infinity values");
    }
    
    @Test
    @DisplayName("Test 7: Range rule with NaN should fail")
    void testRangeRuleWithNaN() throws Exception {
        String rulesJson = """
        [
            {"name": "badRange", "type": "range", "column": "value", "min": "0", "max": "NaN"}
        ]
        """;
        
        List<Map<String, Object>> rules = validator.load(rulesJson);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        assertFalse(result.isValid(), "Range with NaN should fail");
        assertTrue(result.getErrors().stream()
            .anyMatch(e -> e.getMessage().contains("finite number")),
            "Should reject NaN values");
    }
    
    @Test
    @DisplayName("Test 8: Decimal rule defaults")
    void testDecimalRuleDefaults() throws Exception {
        String rulesJson = """
        [
            {"name": "decimalDefault", "type": "decimal", "column": "amount"}
        ]
        """;
        
        List<Map<String, Object>> rules = validator.load(rulesJson);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        assertTrue(result.isValid(), "Decimal rule with defaults should pass");
        
        // Verify defaults were applied
        Map<String, Object> rule = rules.get(0);
        assertEquals(18, rule.get("precision"), "Should default precision to 18");
        assertEquals(2, rule.get("scale"), "Should default scale to 2");
        assertEquals(false, rule.get("exact_scale"), "Should default exact_scale to false");
    }
    
    @Test
    @DisplayName("Test 9: Enum rule alias normalization")
    void testEnumRuleAliasNormalization() throws Exception {
        String rulesJson = """
        [
            {"name": "currencyEnum", "type": "enum", "column": "currency", "allowedValues": ["USD", "CAD"]}
        ]
        """;
        
        List<Map<String, Object>> rules = validator.load(rulesJson);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        assertTrue(result.isValid(), "Enum rule with allowedValues should pass");
        
        // Verify alias was normalized
        Map<String, Object> rule = rules.get(0);
        assertNotNull(rule.get("allowed"), "Should normalize allowedValues to allowed");
    }
    
    @Test
    @DisplayName("Test 10: Length rule with invalid integer values")
    void testLengthRuleInvalidIntegers() throws Exception {
        String rulesJson = """
        [
            {"name": "lengthBad", "type": "length", "column": "name", "min": "abc", "max": "xyz"}
        ]
        """;
        
        List<Map<String, Object>> rules = validator.load(rulesJson);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        assertFalse(result.isValid(), "Length rule with non-integer values should fail");
        assertTrue(result.getErrors().stream()
            .anyMatch(e -> e.getMessage().contains("valid integers")),
            "Should reject non-integer min/max");
    }
    
    @Test
    @DisplayName("Test 11: Decimal rule with invalid boolean")
    void testDecimalRuleInvalidBoolean() throws Exception {
        String rulesJson = """
        [
            {"name": "decimalBad", "type": "decimal", "column": "amount", "exact_scale": "maybe"}
        ]
        """;
        
        List<Map<String, Object>> rules = validator.load(rulesJson);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        assertFalse(result.isValid(), "Decimal rule with invalid boolean should fail");
        assertTrue(result.getErrors().stream()
            .anyMatch(e -> e.getMessage().contains("must be boolean")),
            "Should reject non-boolean exact_scale");
    }
    
    @Test
    @DisplayName("Test 12: All supported rule types")
    void testAllSupportedRuleTypes() throws Exception {
        String rulesJson = """
        [
            {"name": "r1", "type": "headers", "columns": ["col1"]},
            {"name": "r2", "type": "non_empty", "columns": ["col1"]},
            {"name": "r3", "type": "range", "column": "col1", "min": 0, "max": 100},
            {"name": "r4", "type": "enum", "column": "col1", "allowed": ["A", "B"]},
            {"name": "r5", "type": "length", "column": "col1", "min": 1, "max": 10},
            {"name": "r6", "type": "regex", "column": "col1", "pattern": "^[A-Z]+$"},
            {"name": "r7", "type": "unique", "columns": ["col1", "col2"]},
            {"name": "r8", "type": "decimal", "column": "col1", "precision": 10, "scale": 2}
        ]
        """;
        
        List<Map<String, Object>> rules = validator.load(rulesJson);
        RuleSchemaValidator.ValidationResult result = validator.validate(rules);
        
        assertTrue(result.isValid(), "All supported rule types should pass");
        assertEquals(8, rules.size(), "Should have 8 rules");
        assertEquals(0, result.getErrors().size(), "Should have no errors");
    }
}

package com.validators;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validates and normalizes rule schema JSON for data validation.
 * Supports 8 rule types: headers, non_empty, range, enum, length, regex, unique, decimal.
 * 
 * Defaults: decimal.precision=18, decimal.scale=2, exact_scale=false.
 */
public class RuleSchemaValidator {
    
    private static final Set<String> SUPPORTED_TYPES = Set.of(
        "headers", "non_empty", "range", "enum", "length", "regex", "unique", "decimal"
    );
    
    private final int defaultPrecision;
    private final int defaultScale;
    private final ObjectMapper objectMapper;
    private final Map<String, RuleValidator> validators;
    
    /**
     * Functional interface for rule-specific validation logic.
     */
    @FunctionalInterface
    private interface RuleValidator {
        void validate(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues);
    }
    
    /**
     * Initialize RuleSchemaValidator with default decimal precision/scale and rule type dispatch table.
     */
    public RuleSchemaValidator() {
        this.defaultPrecision = 18;
        this.defaultScale = 2;
        this.objectMapper = new ObjectMapper();
        
        // Dispatcher: rule.type -> validator method
        this.validators = new HashMap<>();
        validators.put("headers", this::validateHeadersRule);
        validators.put("non_empty", this::validateNonEmptyRule);
        validators.put("range", this::validateRangeRule);
        validators.put("enum", this::validateEnumRule);
        validators.put("length", this::validateLengthRule);
        validators.put("regex", this::validateRegexRule);
        validators.put("unique", this::validateUniqueRule);
        validators.put("decimal", this::validateDecimalRule);
    }
    
    // ---------- Public API ----------
    
    /**
     * Load rules from a JSON string. Throws IllegalArgumentException if the root is not a list.
     * 
     * @param jsonText JSON string representing rules
     * @return List of rule maps
     * @throws Exception if JSON parsing fails
     */
    public List<Map<String, Object>> load(String jsonText) throws Exception {
        Object parsed = objectMapper.readValue(jsonText, Object.class);
        if (!(parsed instanceof List)) {
            throw new IllegalArgumentException("Rules JSON must be a list.");
        }
        return objectMapper.readValue(jsonText, new TypeReference<List<Map<String, Object>>>() {});
    }
    
    /**
     * Validate a list of rule dictionaries.
     * Checks for supported types, duplicate names, and delegates to per-type validators.
     * Also checks for cross-rule advisories (e.g., multiple 'headers' rules).
     * 
     * @param rules List of rule maps
     * @return ValidationResult containing validity status and list of issues
     */
    public ValidationResult validate(List<Map<String, Object>> rules) {
        List<ValidationIssue> issues = new ArrayList<>();
        Set<String> namesSeen = new HashSet<>();
        int headersCount = 0;
        
        for (int idx = 0; idx < rules.size(); idx++) {
            Map<String, Object> rule = rules.get(idx);
            String path = "[" + idx + "]";
            
            String rtype = rule.getOrDefault("type", "").toString().trim();
            String rname = rule.getOrDefault("name", "").toString().trim();
            if (rname.isEmpty()) {
                rname = "rule_" + idx;
            }
            rule.put("name", rname);
            
            // Check if type is supported
            if (!SUPPORTED_TYPES.contains(rtype)) {
                issues.add(err(rname, rtype, path, 
                    String.format("Unsupported type '%s'. Supported: %s", rtype, SUPPORTED_TYPES)));
                continue;
            }
            
            // Check for duplicate names
            if (namesSeen.contains(rname)) {
                issues.add(err(rname, rtype, path, "Duplicate rule name"));
            }
            namesSeen.add(rname);
            
            if ("headers".equals(rtype)) {
                headersCount++;
            }
            
            // Dispatch to per-rule method
            RuleValidator validator = validators.get(rtype);
            if (validator != null) {
                validator.validate(rule, rname, rtype, path, issues);
            }
        }
        
        // Cross-rule advisories
        if (headersCount > 1) {
            issues.add(warn("<schema>", "headers", "$", 
                "Multiple 'headers' rules present; consider consolidating."));
        }
        
        // Determine if validation was successful (no errors)
        long errorCount = issues.stream().filter(ValidationIssue::isError).count();
        boolean isValid = errorCount == 0;
        
        return new ValidationResult(isValid, issues);
    }
    
    // ---------- Per-rule validators (one method per type) ----------
    
    /**
     * Validates a 'headers' rule. Requires a non-empty list of column names in 'columns'.
     */
    private void validateHeadersRule(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues) {
        requireList(rule, "columns", path, rname, rtype, issues);
    }
    
    /**
     * Validates a 'non_empty' rule. Requires a non-empty list of column names in 'columns'.
     */
    private void validateNonEmptyRule(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues) {
        requireList(rule, "columns", path, rname, rtype, issues);
    }
    
    /**
     * Validates a 'range' rule. Requires 'column', 'min', and 'max' keys.
     * Checks that min and max are numeric, min <= max, and values are within double bounds.
     * 
     * Valid examples (will pass):
     *   {"type": "range", "column": "age", "min": "0", "max": "120"}
     *   {"type": "range", "column": "price", "min": "0.99", "max": "999.99"}
     *   {"type": "range", "column": "temperature", "min": "-40", "max": "50"}
     *   {"type": "range", "column": "score", "min": "0.0", "max": "100.0"}
     *   {"type": "range", "column": "distance", "min": "1.5e10", "max": "3.0e12"}
     * 
     * Invalid examples (will be rejected):
     *   {"type": "range", "column": "value", "min": "0", "max": "Infinity"}
     *     -> Error: "max value must be a finite number"
     *   {"type": "range", "column": "value", "min": "-Infinity", "max": "100"}
     *     -> Error: "min value must be a finite number"
     *   {"type": "range", "column": "score", "min": "0", "max": "NaN"}
     *     -> Error: "max value must be a finite number"
     *   {"type": "range", "column": "age", "min": "zero", "max": "hundred"}
     *     -> Error: "min/max must be numeric and within valid range"
     *   {"type": "range", "column": "price", "min": "$10", "max": "$100"}
     *     -> Error: "min/max must be numeric and within valid range"
     *   {"type": "range", "column": "age", "min": "65", "max": "18"}
     *     -> Error: "min must be <= max"
     */
    private void validateRangeRule(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues) {
        requireKeys(rule, Arrays.asList("column", "min", "max"), path, rname, rtype, issues);
        
        if (rule.containsKey("min") && rule.containsKey("max")) {
            try {
                double min = Double.parseDouble(rule.get("min").toString());
                double max = Double.parseDouble(rule.get("max").toString());
                
                // Check for reasonable bounds and special values
                if (Double.isInfinite(min) || Double.isNaN(min)) {
                    issues.add(err(rname, rtype, path + ".min", "min value must be a finite number"));
                }
                if (Double.isInfinite(max) || Double.isNaN(max)) {
                    issues.add(err(rname, rtype, path + ".max", "max value must be a finite number"));
                }
                if (min > max) {
                    issues.add(err(rname, rtype, path + ".min/max", "min must be <= max"));
                }
            } catch (NumberFormatException e) {
                issues.add(err(rname, rtype, path + ".min/max", "min/max must be numeric and within valid range"));
            }
        }
    }
    
    /**
     * Validates an 'enum' rule. Requires 'column' key and a non-empty 'allowed' list.
     * Normalizes 'allowedValues' to 'allowed' if present.
     */
    private void validateEnumRule(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues) {
        requireKeys(rule, Collections.singletonList("column"), path, rname, rtype, issues);
        
        // Normalize alias
        if (!rule.containsKey("allowed") && rule.containsKey("allowedValues")) {
            rule.put("allowed", rule.get("allowedValues"));
        }
        
        Object allowed = rule.get("allowed");
        if (!(allowed instanceof List) || ((List<?>) allowed).isEmpty()) {
            issues.add(err(rname, rtype, path + ".allowed", "Provide non-empty 'allowed' list"));
        }
    }
    
    /**
     * Validates a 'length' rule. Requires 'column' key and checks min/max constraints.
     * Sets defaults for min (0) and max (255) if not provided.
     */
    private void validateLengthRule(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues) {
        requireKeys(rule, Collections.singletonList("column"), path, rname, rtype, issues);
        
        int min = 0;
        int max = 255;
        
        try {
            if (rule.containsKey("min")) {
                min = Integer.parseInt(rule.get("min").toString());
            }
            if (rule.containsKey("max")) {
                max = Integer.parseInt(rule.get("max").toString());
            }
        } catch (NumberFormatException e) {
            issues.add(err(rname, rtype, path + ".min/max", "min/max must be valid integers"));
            return;
        }
        
        rule.put("min", min);
        rule.put("max", max);
        
        if (min < 0 || max < 0 || min > max) {
            issues.add(err(rname, rtype, path + ".min/max", "0 ≤ min ≤ max required"));
        }
    }
    
    /**
     * Validates a 'regex' rule. Requires 'column' and 'pattern' keys.
     * Checks that the regex pattern is valid.
     */
    private void validateRegexRule(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues) {
        requireKeys(rule, Arrays.asList("column", "pattern"), path, rname, rtype, issues);
        
        try {
            String pattern = rule.getOrDefault("pattern", "").toString();
            Pattern.compile(pattern);
        } catch (PatternSyntaxException e) {
            issues.add(err(rname, rtype, path + ".pattern", "Invalid regex: " + e.getMessage()));
        }
    }
    
    /**
     * Validates a 'unique' rule. Requires a non-empty list of column names in 'columns'.
     */
    private void validateUniqueRule(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues) {
        requireList(rule, "columns", path, rname, rtype, issues);
    }
    
    /**
     * Validates a 'decimal' rule. Requires 'column' key and checks precision/scale constraints.
     * Sets defaults for precision and scale if not provided. Checks that min/max are numeric if present.
     * 
     * Valid examples (will pass):
     *   {"type": "decimal", "column": "amount", "precision": 10, "scale": 2}
     *   {"type": "decimal", "column": "price", "precision": 18, "scale": 6}
     *   {"type": "decimal", "column": "value", "precision": 10, "scale": 2, "min": -1000, "max": 1000}
     *   {"type": "decimal", "column": "balance", "precision": 15, "scale": 2, "exact_scale": true}
     *   {"type": "decimal", "column": "rate"}  // Uses defaults: precision=18, scale=2
     * 
     * Invalid examples (will be rejected):
     *   {"type": "decimal", "column": "amount", "precision": 0, "scale": 2}
     *     -> Error: "Require precision>0 and 0≤scale≤precision"
     *   {"type": "decimal", "column": "amount", "precision": 10, "scale": -1}
     *     -> Error: "Require precision>0 and 0≤scale≤precision"
     *   {"type": "decimal", "column": "amount", "precision": 5, "scale": 10}
     *     -> Error: "Require precision>0 and 0≤scale≤precision"
     *   {"type": "decimal", "column": "amount", "precision": 20, "scale": 2}
     *     -> Error: "Precision must not exceed 18 (financial standard)"
     *   {"type": "decimal", "column": "amount", "precision": 10, "scale": 8}
     *     -> Error: "Scale must not exceed 6 (common practice)"
     *   {"type": "decimal", "column": "amount", "precision": 10, "scale": 2, "min": "abc"}
     *     -> Error: "'min' must be numeric if provided"
     *   {"type": "decimal", "column": "amount", "precision": 10, "scale": 2, "max": "xyz"}
     *     -> Error: "'max' must be numeric if provided"
     */
    private void validateDecimalRule(Map<String, Object> rule, String rname, String rtype, String path, List<ValidationIssue> issues) {
        requireKeys(rule, Collections.singletonList("column"), path, rname, rtype, issues);
        
        // Defaults
        int precision = defaultPrecision;
        int scale = defaultScale;
        
        try {
            if (rule.containsKey("precision")) {
                precision = Integer.parseInt(rule.get("precision").toString());
            }
            if (rule.containsKey("scale")) {
                scale = Integer.parseInt(rule.get("scale").toString());
            }
        } catch (NumberFormatException e) {
            issues.add(err(rname, rtype, path + ".precision/scale", "precision/scale must be valid integers"));
            return;
        }
        
        rule.put("precision", precision);
        rule.put("scale", scale);
        
        // Parse exact_scale with validation
        boolean exactScale = false;
        if (rule.containsKey("exact_scale")) {
            Object exactScaleValue = rule.get("exact_scale");
            if (exactScaleValue instanceof Boolean) {
                exactScale = (Boolean) exactScaleValue;
            } else {
                String strValue = exactScaleValue.toString().toLowerCase();
                if ("true".equals(strValue)) {
                    exactScale = true;
                } else if (!"false".equals(strValue)) {
                    issues.add(err(rname, rtype, path + ".exact_scale", "exact_scale must be boolean (true/false)"));
                    return;
                }
            }
        }
        rule.put("exact_scale", exactScale);
        
        if (precision <= 0 || scale < 0 || scale > precision) {
            issues.add(err(rname, rtype, path + ".precision/scale", 
                "Require precision>0 and 0≤scale≤precision"));
        }
        if (precision > 18) {
            issues.add(err(rname, rtype, path + ".precision", 
                "Precision must not exceed 18 (financial standard)"));
        }
        if (scale > 6) {
            issues.add(err(rname, rtype, path + ".scale", 
                "Scale must not exceed 6 (common practice)"));
        }
        
        for (String bound : Arrays.asList("min", "max")) {
            if (rule.containsKey(bound) && !isNumber(rule.get(bound))) {
                issues.add(err(rname, rtype, path + "." + bound, 
                    String.format("'%s' must be numeric if provided", bound)));
            }
        }
    }
    
    // ---------- Helpers ----------
    
    /**
     * Helper to ensure required keys are present in a rule object.
     * Adds an error to issues for each missing key.
     */
    private void requireKeys(Map<String, Object> obj, List<String> keys, String path, 
                            String rname, String rtype, List<ValidationIssue> issues) {
        for (String key : keys) {
            if (!obj.containsKey(key)) {
                issues.add(err(rname, rtype, path + "." + key, 
                    String.format("Missing required key '%s'", key)));
            }
        }
    }
    
    /**
     * Helper to ensure a key in a rule object is a non-empty list of strings.
     * Adds an error to issues if not satisfied.
     */
    private void requireList(Map<String, Object> obj, String key, String path, 
                            String rname, String rtype, List<ValidationIssue> issues) {
        Object val = obj.get(key);
        boolean ok = val instanceof List 
            && !((List<?>) val).isEmpty() 
            && ((List<?>) val).stream()
                .allMatch(x -> x instanceof String && !((String) x).trim().isEmpty());
        
        if (!ok) {
            issues.add(err(rname, rtype, path + "." + key, 
                String.format("Provide non-empty list of strings in '%s'", key)));
        }
    }
    
    /**
     * Returns true if v can be converted to a number, false otherwise.
     */
    private static boolean isNumber(Object v) {
        try {
            new BigDecimal(v.toString());
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Helper to create an ERROR-level ValidationIssue.
     */
    private static ValidationIssue err(String ruleName, String ruleType, String path, String msg) {
        return new ValidationIssue(ruleName, ruleType, path, "ERROR", msg);
    }
    
    /**
     * Helper to create a WARN-level ValidationIssue.
     */
    private static ValidationIssue warn(String ruleName, String ruleType, String path, String msg) {
        return new ValidationIssue(ruleName, ruleType, path, "WARN", msg);
    }
    
    /**
     * Result object containing validation status and issues.
     */
    public static class ValidationResult {
        private final boolean valid;
        private final List<ValidationIssue> issues;
        
        public ValidationResult(boolean valid, List<ValidationIssue> issues) {
            this.valid = valid;
            this.issues = Collections.unmodifiableList(issues);
        }
        
        public boolean isValid() {
            return valid;
        }
        
        public List<ValidationIssue> getIssues() {
            return issues;
        }
        
        public List<ValidationIssue> getErrors() {
            return issues.stream()
                .filter(ValidationIssue::isError)
                .toList();
        }
        
        public List<ValidationIssue> getWarnings() {
            return issues.stream()
                .filter(ValidationIssue::isWarning)
                .toList();
        }
    }
}

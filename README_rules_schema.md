# Rules Schema Design and Usage Guide

This document explains the design of the rules schema used for data validation in this project, how to create your own schema files, support for different file extensions, multi-layered schemas, and how schema validation works.

## 1. Overview

The rules schema defines the validation logic for datasets (CSV, JSON, Parquet, etc.) processed by the PySpark validation framework. It specifies constraints, expected columns, data types, and custom rules to ensure data quality and integrity.

## 2. Schema File Format and Extensions

Schemas are currently stored ONLY as JSON files. Other serialization formats (YAML, TOML) are not implemented in this codebase. If you see references to alternative formats, treat them as future possibilities rather than supported features.

- `.json` — Rules schema (list of rule objects)

**Example filenames:**
- `rules.json` — Standard rules schema
- `rules_relaxed.json` — Relaxed rules schema (empty or minimal set of rules)

### Extensibility (Future)
To add support for YAML or TOML you would:
1. Parse the file into a Python list of dicts identical to the JSON structure.
2. Pass that list directly to the existing validators (no additional transformation needed).
3. Add a lightweight loader helper (e.g. `load_rules_yaml(path)`).

Until that is implemented, keep all schema definitions in JSON.

## 3. Basic Schema Structure


A rules schema is a list of rule objects, each specifying a validation type and its parameters. Each rule object must include a `type` field, which determines how the rule is applied. Below are the supported rule types and how to write them:

### Rule Types and Examples

#### 1. `headers`
Ensures required columns are present in the DataFrame.
```json
{
  "name": "headers_check",
  "type": "headers",
  "columns": ["portfolio", "inventory", "value"]
}
```

#### 2. `non_empty`
Checks that specified columns are not null or empty strings.
```json
{
  "name": "required_fields",
  "type": "non_empty",
  "columns": ["portfolio", "inventory"]
}
```

#### 3. `range`
Validates that a column's numeric values fall within a specified range.
```json
{
  "name": "value_range",
  "type": "range",
  "column": "value",
  "min": -100,
  "max": 100
}
```

#### 4. `enum`
Checks that a column's value is one of the allowed values.
```json
{
  "name": "currency_enum",
  "type": "enum",
  "column": "currency",
  "allowed": ["CAD", "USD", "EUR"]
}
```

#### 5. `length`
Validates the length of string values in a column.
```json
{
  "name": "inventory_length",
  "type": "length",
  "column": "inventory",
  "min": 5,
  "max": 30
}
```

#### 6. `regex`
Checks that a column's value matches a regular expression pattern.
```json
{
  "name": "risk_metric_pattern",
  "type": "regex",
  "column": "riskMetric",
  "pattern": "^(IR_DELTA|IR_VEGA|CR01)$"
}
```

#### 7. `unique`
Ensures that the combination of specified columns is unique (no duplicate rows).
```json
{
  "name": "unique_portfolio_inventory",
  "type": "unique",
  "columns": ["portfolio", "inventory"]
}
```

#### 8. `decimal`
Validates that a column's value fits a specified decimal precision and scale, with optional min/max and exact scale enforcement.
```json
{
  "name": "value_decimal",
  "type": "decimal",
  "column": "value",
  "precision": 18,
  "scale": 6,
  "exact_scale": true,
  "min": -100000,
  "max": 100000
}
```

### Rule Object Fields
- `name`: (optional) A label for the rule, used in error reporting.
- `type`: The rule type (see above).
- `columns` or `column`: The target column(s) for the rule.
- Other fields depend on the rule type (e.g., `min`, `max`, `allowed`, `pattern`, `precision`, `scale`).

### Example: Full Rules List
```json
[
  {
    "name": "headers_check",
    "type": "headers",
    "columns": ["portfolio", "inventory", "value"]
  },
  {
    "name": "required_fields",
    "type": "non_empty",
    "columns": ["portfolio", "inventory"]
  },
  {
    "name": "value_range",
    "type": "range",
    "column": "value",
    "min": -100,
    "max": 100
  }
]
```

---

## 4. Multi-Layered Schemas

Multi-layered schemas can be constructed by combining multiple rules, or by nesting rules for hierarchical or complex data (such as nested JSON objects). For most tabular data, rules are applied at the top level. For nested structures, you may define rules for subfields using custom rule types or by extending the validator.

**Example: Multi-layered rules list**
```json
[
  {
    "name": "headers_check",
    "type": "headers",
    "columns": ["user.id", "user.name", "timestamp"]
  },
  {
    "name": "user_id_required",
    "type": "non_empty",
    "columns": ["user.id"]
  },
  {
    "name": "timestamp_format",
    "type": "regex",
    "column": "timestamp",
    "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
  }
]
```

For advanced nested validation, you may need to extend the validator to support custom rule types for objects or arrays.

## 5. Creating a Schema File

1. List all validation rules as objects in a JSON array.
2. For each rule, specify the `type` and required parameters.
3. Save the schema as a `.json` file in the appropriate rules directory (e.g., `tests/rules/`).
4. For multi-layered schemas, combine rules for nested fields as needed.

**Example: Relaxed Schema (allows all data):**
```json
[]
```
An empty rules list means no validation is applied (all data passes).

## 6. Validation Workflow


1. **Schema Loading:** The validator loads the rules schema (JSON array of rule objects).
2. **Schema Validation:** The `RuleSchemaValidator` checks each rule for valid structure and supported types.
3. **Data Validation:** The `SparkDataValidator` applies each rule in order:
  - `headers` rules are checked first (missing columns cause immediate failure if `fail_fast` is enabled).
  - Other rules are applied to the data, collecting violations.
  - If `fail_fast` is enabled, validation stops at the first error (optionally raises an exception).
  - Otherwise, all violations are collected and returned.
  - The result is a tuple: `(is_valid, valid_df, errors_df)`.
4. **Error Reporting:** Any schema or data errors are reported with details including rule name, column, value, and message.

## 7. Best Practices
- Always validate your schema with the `RuleSchemaValidator` before using it for data validation.
- Use descriptive constraint keys and document custom rules.
- For complex datasets, use multi-layered schemas to capture nested structures.
- Store schemas in a version-controlled directory for reproducibility.

## 8. Example Directory Structure
```
tests/
  rules/
    rules.json
    rules_relaxed.json
    sample_json_rules.json
    sample_parquet_rules.json
```

## 9. References
- See `validator.py` for schema validation logic.
- See `test_validators.py` for examples of schema usage in tests.

---
For further details or custom schema requirements, refer to the code documentation or contact the project maintainers.

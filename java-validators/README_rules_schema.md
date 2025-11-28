# Rules Schema Design and Usage Guide

This document explains the design of the rules schema used for data validation in this project, how to create your own schema files, support for different file extensions, multi-layered schemas, and how schema validation works.

## 1. Overview

The rules schema defines the validation logic for datasets (CSV, JSON, Parquet, etc.) processed by the PySpark validation framework. It specifies constraints, expected columns, data types, and custom rules to ensure data quality and integrity.

The `RuleSchemaValidator` validates rule schemas before they are applied to data, ensuring:
- All required fields are present
- Data types are correct
- Values are within valid ranges
- No duplicate rule names
- Supported rule types only

**Supported Rule Types**: `headers`, `non_empty`, `range`, `enum`, `length`, `regex`, `unique`, `decimal`

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

A rules schema is a JSON array of rule objects. Each rule object must include:
- `type` (required): The validation rule type
- `name` (optional): A descriptive identifier for the rule (auto-generated if omitted)
- Additional fields specific to the rule type

**Schema Validation**:
- Duplicate rule names are flagged as errors
- Unsupported rule types are rejected
- Missing required fields are caught before data validation
- Multiple `headers` rules generate a warning (consider consolidating)

Below are the supported rule types and how to write them:

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

**Required fields**: `column`, `min`, `max`

**Validation checks**:
- `min` and `max` must be numeric (parseable as float/double)
- `min` must be ≤ `max`
- Values must be within float bounds: ±1.7976931348623157e308
- Java: Uses `Double.isInfinite()` and `Double.isNaN()` checks

```json
{
  "name": "value_range",
  "type": "range",
  "column": "value",
  "min": -100,
  "max": 100
}
```

**Valid examples**:
```json
{"type": "range", "column": "age", "min": 0, "max": 120}
{"type": "range", "column": "price", "min": 0.99, "max": 999.99}
{"type": "range", "column": "temperature", "min": -40, "max": 50}
{"type": "range", "column": "distance", "min": 1.5e10, "max": 3.0e12}
{"type": "range", "column": "extreme", "min": -1e308, "max": 1e308}
```

**Invalid examples** (will be rejected):
```json
{"type": "range", "column": "value", "min": 0, "max": 2e308}
// Python Error: "max value exceeds float bounds"
// Java Error: "max value must be a finite number"

{"type": "range", "column": "value", "min": -2e308, "max": 100}
// Python Error: "min value exceeds float bounds"
// Java Error: "min value must be a finite number"

{"type": "range", "column": "age", "min": "zero", "max": "hundred"}
// Error: "min/max must be numeric and within valid range"

{"type": "range", "column": "age", "min": 65, "max": 18}
// Error: "min must be <= max"
```

#### 4. `enum`
Checks that a column's value is one of the allowed values.

**Required fields**: `column`, `allowed` (or `allowedValues`)

**Validation checks**:
- `allowed` must be a non-empty list
- Alias normalization: `allowedValues` is automatically converted to `allowed`

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

**Required fields**: `column`

**Optional fields**: `min` (default: 0), `max` (defaut: 255, common practice for varchar limits)

**Validation checks**:
- `min` and `max` must be valid integers
- `min` ≥ 0
- `max` ≥ 0
- `min` ≤ `max`
- Default max of 255 follows common database varchar practices

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

**Required fields**: `column`, `pattern`

**Validation checks**:
- `pattern` must be a valid regular expression (compilable)
- Invalid patterns are caught during schema validation
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

## 4. Multi-Layered Schemas (Nested JSON Support)

For nested JSON structures, use dot notation to reference nested fields. The validator supports validation of nested columns at any depth. Multi-layered schemas can be constructed by combining multiple rules for hierarchical or complex data (such as nested JSON objects). For most tabular data, rules are applied at the top level.

**Example: Nested JSON structure**
```json
{
  "dealRid": "DEAL123",
  "facilityRid": "FAC456",
  "dailyPnl": {
    "tradingPnlAmt": 12345.67,
    "interestAmt": 890.12,
    "totalPnlAmt": 13235.79
  },
  "positions": {
    "symbol": "AAPL",
    "qty": 100,
    "avgPrice": 150.25,
    "currency": "USD"
  }
}
```

**Validation rules for nested structure**:
```json
[
  {
    "name": "headers_check",
    "type": "headers",
    "columns": [
      "dealRid",
      "facilityRid",
      "dailyPnl.tradingPnlAmt",
      "dailyPnl.totalPnlAmt",
      "positions.symbol",
      "positions.currency"
    ]
  },
  {
    "name": "required_nested_fields",
    "type": "non_empty",
    "columns": [
      "dealRid",
      "positions.symbol"
    ]
  },
  {
    "name": "trading_pnl_range",
    "type": "range",
    "column": "dailyPnl.tradingPnlAmt",
    "min": -1000000,
    "max": 1000000
  },
  {
    "name": "timestamp_format",
    "type": "regex",
    "column": "timestamp",
    "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
  }
]
```

**Key points**:
- Use dot notation (`parent.child`) for nested fields
- Supports unlimited nesting depth
- All rule types work with nested columns
- Backtick escaping in implementation handles column names with dots

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
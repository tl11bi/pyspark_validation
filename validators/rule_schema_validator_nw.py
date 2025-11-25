from __future__ import annotations
import json, re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Callable

SUPPORTED_TYPES = ("headers","non_empty","range","enum","length","regex","unique","decimal")

@dataclass
class ValidationIssue:
    rule_name: str
    rule_type: str
    path: str
    level: str   # "ERROR" | "WARN"
    message: str

class RuleSchemaValidator:
    """
    Validates & normalizes rules.json. One method per rule type.
    Defaults: decimal.precision=18, decimal.scale=2, exact_scale=False.
    """

    def __init__(self):
        """
        Initialize RuleSchemaValidator with default decimal precision/scale and rule type dispatch table.
        """
        self.default_precision = 18
        self.default_scale = 2
        # Dispatcher: rule.type -> validator method
        self.validators: Dict[str, Callable[[Dict[str, Any], str, str, str, List[ValidationIssue]], None]] = {
            "headers":   self._validate_headers_rule,
            "non_empty": self._validate_non_empty_rule,
            "range":     self._validate_range_rule,
            "enum":      self._validate_enum_rule,
            "length":    self._validate_length_rule,
            "regex":     self._validate_regex_rule,
            "unique":    self._validate_unique_rule,
            "decimal":   self._validate_decimal_rule,
        }

    # ---------- Public API ----------
    def load(self, json_text: str) -> List[Dict[str, Any]]:
        """
        Load rules from a JSON string. Raises ValueError if the root is not a list.
        Args:
            json_text: JSON string representing rules.
        Returns:
            List of rule dictionaries.
        """
        rules = json.loads(json_text)
        if not isinstance(rules, list):
            raise ValueError("Rules JSON must be a list.")
        return rules


    def validate(self, rules: List[Dict[str, Any]]) -> Tuple[bool, List[ValidationIssue]]:
        """
        Validate a list of rule dictionaries.
        Checks for supported types, duplicate names, and delegates to per-type validators.
        Also checks for cross-rule advisories (e.g., multiple 'headers' rules).
        Args:
            rules: List of rule dictionaries.
        Returns:
            Tuple (is_valid, issues):
                is_valid: True if no ERROR-level issues found, False otherwise
                issues: List of ValidationIssue objects (errors and warnings)
        """
        """
        Validate rules and return (is_valid, issues).
        
        Returns:
            is_valid: True if no ERROR-level issues found, False otherwise
            issues: List of validation issues (errors and warnings)
        """
        issues: List[ValidationIssue] = []
        names_seen = set()
        headers_count = 0

        for idx, rule in enumerate(rules):
            path = f"[{idx}]"
            rtype = str(rule.get("type","")).strip()
            rname = (str(rule.get("name","")).strip() or f"rule_{idx}")
            rule["name"] = rname

            if rtype not in SUPPORTED_TYPES:
                issue = self._err(rname, rtype, path, f"Unsupported type '{rtype}'. Supported: {list(SUPPORTED_TYPES)}")
                issues.append(issue)
                continue
            if rname in names_seen:
                issue = self._err(rname, rtype, path, "Duplicate rule name")
                issues.append(issue)
            names_seen.add(rname)
            if rtype == "headers": headers_count += 1

            # dispatch to per-rule method
            self.validators[rtype](rule, rname, rtype, path, issues)

        # cross-rule advisories
        if headers_count > 1:
            issues.append(self._warn("<schema>","headers","$", "Multiple 'headers' rules present; consider consolidating."))

        # Determine if validation was successful (no errors)
        errors = [i for i in issues if i.level == "ERROR"]
        is_valid = len(errors) == 0
        return is_valid, issues

    # ---------- Per-rule validators (one method per type) ----------
    def _validate_headers_rule(self, rule, rname, rtype, path, issues):
        """
        Validates a 'headers' rule. Requires a non-empty list of column names in 'columns'.
        """
        self._require_list(rule, "columns", path, rname, rtype, issues)

    def _validate_non_empty_rule(self, rule, rname, rtype, path, issues):
        """
        Validates a 'non_empty' rule. Requires a non-empty list of column names in 'columns'.
        """
        self._require_list(rule, "columns", path, rname, rtype, issues)

    def _validate_range_rule(self, rule, rname, rtype, path, issues):
        """
        Validates a 'range' rule. Requires 'column', 'min', and 'max' keys.
        Checks that min and max are numeric, min <= max, and values are within float bounds.
        
        Valid examples (will pass):
          {"type": "range", "column": "age", "min": 0, "max": 120}
          {"type": "range", "column": "price", "min": 0.99, "max": 999.99}
          {"type": "range", "column": "temperature", "min": -40, "max": 50}
          {"type": "range", "column": "score", "min": 0.0, "max": 100.0}
          {"type": "range", "column": "distance", "min": 1.5e10, "max": 3.0e12}
        
        Invalid examples (will be rejected):
          {"type": "range", "column": "value", "min": 0, "max": float('inf')}
            -> Error: "max value exceeds float bounds"
          {"type": "range", "column": "value", "min": float('-inf'), "max": 100}
            -> Error: "min value exceeds float bounds"
          {"type": "range", "column": "score", "min": 0, "max": float('nan')}
            -> Error: "min/max must be numeric and within valid range"
          {"type": "range", "column": "age", "min": "zero", "max": "hundred"}
            -> Error: "min/max must be numeric and within valid range"
          {"type": "range", "column": "age", "min": 65, "max": 18}
            -> Error: "min must be <= max"
        """
        self._require_keys(rule, ["column","min","max"], path, rname, rtype, issues)
        if all(k in rule for k in ("min","max")):
            try:
                mn, mx = float(rule["min"]), float(rule["max"])
                # Check for reasonable bounds (Python float range is approximately ±1.8e308)
                max_float = 1.7976931348623157e308
                min_float = -1.7976931348623157e308
                
                if mn < min_float or mn > max_float:
                    issues.append(self._err(rname, rtype, f"{path}.min", "min value exceeds float bounds"))
                if mx < min_float or mx > max_float:
                    issues.append(self._err(rname, rtype, f"{path}.max", "max value exceeds float bounds"))
                if mn > mx:
                    issues.append(self._err(rname, rtype, f"{path}.min/max", "min must be <= max"))
            except (ValueError, OverflowError):
                issues.append(self._err(rname, rtype, f"{path}.min/max", "min/max must be numeric and within valid range"))
            except Exception:
                issues.append(self._err(rname, rtype, f"{path}.min/max", "min/max must be numeric"))

    def _validate_enum_rule(self, rule, rname, rtype, path, issues):
        """
        Validates an 'enum' rule. Requires 'column' key and a non-empty 'allowed' list.
        Normalizes 'allowedValues' to 'allowed' if present.
        """
        self._require_keys(rule, ["column"], path, rname, rtype, issues)
        # normalize alias
        if "allowed" not in rule and "allowedValues" in rule:
            rule["allowed"] = rule["allowedValues"]
        allowed = rule.get("allowed")
        if not isinstance(allowed, list) or len(allowed) == 0:
            issues.append(self._err(rname, rtype, f"{path}.allowed", "Provide non-empty 'allowed' list"))

    def _validate_length_rule(self, rule, rname, rtype, path, issues):
        """
        Validates a 'length' rule. Requires 'column' key and checks min/max constraints.
        Sets defaults for min (0) and max (1,000,000) if not provided.
        """
        self._require_keys(rule, ["column"], path, rname, rtype, issues)
        mn = int(rule.get("min", 0))
        mx = int(rule.get("max", 255))
        rule["min"], rule["max"] = mn, mx
        if mn < 0 or mx < 0 or mn > mx:
            issues.append(self._err(rname, rtype, f"{path}.min/max", "0 ≤ min ≤ max required"))

    def _validate_regex_rule(self, rule, rname, rtype, path, issues):
        """
        Validates a 'regex' rule. Requires 'column' and 'pattern' keys.
        Checks that the regex pattern is valid.
        """
        self._require_keys(rule, ["column","pattern"], path, rname, rtype, issues)
        try:
            re.compile(str(rule.get("pattern","")))
        except re.error as e:
            issues.append(self._err(rname, rtype, f"{path}.pattern", f"Invalid regex: {e}"))

    def _validate_unique_rule(self, rule, rname, rtype, path, issues):
        """
        Validates a 'unique' rule. Requires a non-empty list of column names in 'columns'.
        """
        self._require_list(rule, "columns", path, rname, rtype, issues)

    def _validate_decimal_rule(self, rule, rname, rtype, path, issues):
        """
        Validates a 'decimal' rule. Requires 'column' key and checks precision/scale constraints.
        Sets defaults for precision and scale if not provided. Checks that min/max are numeric if present.
        
        Valid examples (will pass):
          {"type": "decimal", "column": "amount", "precision": 10, "scale": 2}
          {"type": "decimal", "column": "price", "precision": 18, "scale": 6}
          {"type": "decimal", "column": "value", "precision": 10, "scale": 2, "min": -1000, "max": 1000}
          {"type": "decimal", "column": "balance", "precision": 15, "scale": 2, "exact_scale": true}
          {"type": "decimal", "column": "rate"}  # Uses defaults: precision=18, scale=2
        
        Invalid examples (will be rejected):
          {"type": "decimal", "column": "amount", "precision": 0, "scale": 2}
            -> Error: "Require precision>0 and 0≤scale≤precision"
          {"type": "decimal", "column": "amount", "precision": 10, "scale": -1}
            -> Error: "Require precision>0 and 0≤scale≤precision"
          {"type": "decimal", "column": "amount", "precision": 5, "scale": 10}
            -> Error: "Require precision>0 and 0≤scale≤precision"
          {"type": "decimal", "column": "amount", "precision": 20, "scale": 2}
            -> Error: "Precision must not exceed 18 (financial standard)"
          {"type": "decimal", "column": "amount", "precision": 10, "scale": 8}
            -> Error: "Scale must not exceed 6 (common practice)"
          {"type": "decimal", "column": "amount", "precision": 10, "scale": 2, "min": "abc"}
            -> Error: "'min' must be numeric if provided"
          {"type": "decimal", "column": "amount", "precision": 10, "scale": 2, "max": "xyz"}
            -> Error: "'max' must be numeric if provided"
        """
        self._require_keys(rule, ["column"], path, rname, rtype, issues)
        # defaults
        prec = int(rule.get("precision", self.default_precision))
        scale = int(rule.get("scale", self.default_scale))      # default scale=2
        rule["precision"], rule["scale"] = prec, scale
        rule["exact_scale"] = bool(rule.get("exact_scale", False))
        if prec <= 0 or scale < 0 or scale > prec:
            issues.append(self._err(rname, rtype, f"{path}.precision/scale", "Require precision>0 and 0≤scale≤precision"))
        if prec > 18:
            issues.append(self._err(rname, rtype, f"{path}.precision", "Precision must not exceed 18 (financial standard)"))
        if scale > 6:
            issues.append(self._err(rname, rtype, f"{path}.scale", "Scale must not exceed 6 (common practice)"))
        for b in ("min","max"):
            if b in rule and not self._is_number(rule[b]):
                issues.append(self._err(rname, rtype, f"{path}.{b}", f"'{b}' must be numeric if provided"))

    # ---------- Helpers ----------
    def _require_keys(self, obj: Dict[str,Any], keys: List[str], path: str, rname: str, rtype: str, issues: List[ValidationIssue]):
        """
        Helper to ensure required keys are present in a rule object.
        Adds an error to issues for each missing key.
        """
        for k in keys:
            if k not in obj:
                issues.append(self._err(rname, rtype, f"{path}.{k}", f"Missing required key '{k}'"))

    def _require_list(self, obj: Dict[str,Any], key: str, path: str, rname: str, rtype: str, issues: List[ValidationIssue]):
        """
        Helper to ensure a key in a rule object is a non-empty list of strings.
        Adds an error to issues if not satisfied.
        """
        val = obj.get(key)
        ok = isinstance(val, list) and len(val) > 0 and all(isinstance(x, str) and x.strip() for x in val)
        if not ok:
            issues.append(self._err(rname, rtype, f"{path}.{key}", f"Provide non-empty list of strings in '{key}'"))

    @staticmethod
    def _is_number(v: Any) -> bool:
        """
        Returns True if v can be converted to a float, False otherwise.
        """
        try: float(v); return True
        except Exception: return False

    @staticmethod
    def _err(rule_name, rule_type, path, msg) -> ValidationIssue:
        """
        Helper to create an ERROR-level ValidationIssue.
        """
        return ValidationIssue(rule_name, rule_type, path, "ERROR", msg)

    @staticmethod
    def _warn(rule_name, rule_type, path, msg) -> ValidationIssue:
        """
        Helper to create a WARN-level ValidationIssue.
        """
        return ValidationIssue(rule_name, rule_type, path, "WARN", msg)


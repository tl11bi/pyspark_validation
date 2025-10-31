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

    def __init__(
        self,
        id_cols: Optional[List[str]] = None,
        fail_fast: bool = False,
        fail_mode: str = "return",  # "return" | "raise"
    ):
        self.dataset_columns = id_cols
        self.fail_fast = fail_fast
        self.fail_mode = fail_mode
        self.default_precision = 18
        self.default_scale = 2
        # Dispatcher: rule.type -> validator method
        self.validators: Dict[str, Callable[[Dict[str, Any], str, str, str, Optional[List[str]], List[ValidationIssue]], None]] = {
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
        rules = json.loads(json_text)
        if not isinstance(rules, list):
            raise ValueError("Rules JSON must be a list.")
        return rules

    def load_relaxed(self, json_text: str) -> List[Dict[str, Any]]:
        cleaned = self._strip_json_comments_and_trailing_commas(json_text)
        return self.load(cleaned)

    def validate(self, rules: List[Dict[str, Any]]) -> Tuple[bool, List[Dict[str, Any]], List[ValidationIssue]]:
        """
        Validate rules and return (is_valid, rules, issues).
        
        Returns:
            is_valid: True if no ERROR-level issues found, False otherwise
            rules: List of validated/normalized rules
            issues: List of validation issues (errors and warnings)
            
        If fail_fast=True:
            - fail_mode='return' returns immediately with first error
            - fail_mode='raise' raises ValueError with error details
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
                if self.fail_fast:
                    if self.fail_mode == "raise":
                        raise ValueError(f"Rule validation failed: {issue.message}")
                    return False, rules, issues
                continue
            if rname in names_seen:
                issue = self._err(rname, rtype, path, "Duplicate rule name")
                issues.append(issue)
                if self.fail_fast:
                    if self.fail_mode == "raise":
                        raise ValueError(f"Rule validation failed: {issue.message}")
                    return False, rules, issues
            names_seen.add(rname)
            if rtype == "headers": headers_count += 1

            # dispatch to per-rule method
            self.validators[rtype](rule, rname, rtype, path, self.dataset_columns, issues)
            
            # Check for errors after each rule if fail_fast is enabled
            if self.fail_fast:
                errors = [i for i in issues if i.level == "ERROR"]
                if errors:
                    if self.fail_mode == "raise":
                        raise ValueError(f"Rule validation failed: {errors[-1].message}")
                    return False, rules, issues

        # cross-rule advisories
        if headers_count == 0 and self.dataset_columns is not None:
            issues.append(self._warn("<schema>","headers","$", "No 'headers' rule present; column presence not enforced."))
        if headers_count > 1:
            issues.append(self._warn("<schema>","headers","$", "Multiple 'headers' rules present; consider consolidating."))

        # Determine if validation was successful (no errors)
        errors = [i for i in issues if i.level == "ERROR"]
        is_valid = len(errors) == 0
        return is_valid, rules, issues

    # ---------- Per-rule validators (one method per type) ----------
    def _validate_headers_rule(self, rule, rname, rtype, path, dataset_columns, issues):
        self._require_list(rule, "columns", path, rname, rtype, issues)
        # Use instance variable if parameter is None
        cols = dataset_columns if dataset_columns is not None else self.dataset_columns
        if cols:
            missing = [c for c in rule.get("columns", []) if c not in cols]
            if missing:
                issues.append(self._warn(rname, rtype, f"{path}.columns", f"Columns not in dataset hint: {missing}"))

    def _validate_non_empty_rule(self, rule, rname, rtype, path, dataset_columns, issues):
        self._require_list(rule, "columns", path, rname, rtype, issues)
        self._warn_unknown(rule, "columns", dataset_columns, rname, rtype, path, issues)

    def _validate_range_rule(self, rule, rname, rtype, path, dataset_columns, issues):
        self._require_keys(rule, ["column","min","max"], path, rname, rtype, issues)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues)
        if all(k in rule for k in ("min","max")):
            try:
                mn, mx = float(rule["min"]), float(rule["max"])
                if mn > mx: issues.append(self._err(rname, rtype, f"{path}.min/max", "min must be <= max"))
            except Exception:
                issues.append(self._err(rname, rtype, f"{path}.min/max", "min/max must be numeric"))

    def _validate_enum_rule(self, rule, rname, rtype, path, dataset_columns, issues):
        self._require_keys(rule, ["column"], path, rname, rtype, issues)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues)
        # normalize alias
        if "allowed" not in rule and "allowedValues" in rule:
            rule["allowed"] = rule["allowedValues"]
        allowed = rule.get("allowed")
        if not isinstance(allowed, list) or len(allowed) == 0:
            issues.append(self._err(rname, rtype, f"{path}.allowed", "Provide non-empty 'allowed' list"))

    def _validate_length_rule(self, rule, rname, rtype, path, dataset_columns, issues):
        self._require_keys(rule, ["column"], path, rname, rtype, issues)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues)
        mn = int(rule.get("min", 0)); mx = int(rule.get("max", 1_000_000))
        rule["min"], rule["max"] = mn, mx
        if mn < 0 or mx < 0 or mn > mx:
            issues.append(self._err(rname, rtype, f"{path}.min/max", "0 ≤ min ≤ max required"))

    def _validate_regex_rule(self, rule, rname, rtype, path, dataset_columns, issues):
        self._require_keys(rule, ["column","pattern"], path, rname, rtype, issues)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues)
        try:
            re.compile(str(rule.get("pattern","")))
        except re.error as e:
            issues.append(self._err(rname, rtype, f"{path}.pattern", f"Invalid regex: {e}"))

    def _validate_unique_rule(self, rule, rname, rtype, path, dataset_columns, issues):
        self._require_list(rule, "columns", path, rname, rtype, issues)
        self._warn_unknown(rule, "columns", dataset_columns, rname, rtype, path, issues)

    def _validate_decimal_rule(self, rule, rname, rtype, path, dataset_columns, issues):
        self._require_keys(rule, ["column"], path, rname, rtype, issues)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues)
        # defaults
        prec = int(rule.get("precision", self.default_precision))
        scale = int(rule.get("scale", self.default_scale))      # default scale=2
        rule["precision"], rule["scale"] = prec, scale
        rule["exact_scale"] = bool(rule.get("exact_scale", False))
        if prec <= 0 or scale < 0 or scale > prec:
            issues.append(self._err(rname, rtype, f"{path}.precision/scale", "Require precision>0 and 0≤scale≤precision"))
        for b in ("min","max"):
            if b in rule and not self._is_number(rule[b]):
                issues.append(self._err(rname, rtype, f"{path}.{b}", f"'{b}' must be numeric if provided"))

    # ---------- Helpers ----------
    def _require_keys(self, obj: Dict[str,Any], keys: List[str], path: str, rname: str, rtype: str, issues: List[ValidationIssue]):
        for k in keys:
            if k not in obj:
                issues.append(self._err(rname, rtype, f"{path}.{k}", f"Missing required key '{k}'"))

    def _require_list(self, obj: Dict[str,Any], key: str, path: str, rname: str, rtype: str, issues: List[ValidationIssue]):
        val = obj.get(key)
        ok = isinstance(val, list) and len(val) > 0 and all(isinstance(x, str) and x.strip() for x in val)
        if not ok:
            issues.append(self._err(rname, rtype, f"{path}.{key}", f"Provide non-empty list of strings in '{key}'"))

    def _warn_unknown(self, obj, key: str, dataset_columns: Optional[List[str]], rname: str, rtype: str, path: str, issues: List[ValidationIssue]):
        # Use instance variable if dataset_columns parameter is None
        cols = dataset_columns if dataset_columns is not None else self.dataset_columns
        if not cols: return
        if key == "column":
            c = obj.get("column")
            if isinstance(c, str) and c not in cols:
                issues.append(self._warn(rname, rtype, f"{path}.column", f"Column '{c}' not in dataset hint"))
        else:  # list
            lst = obj.get(key, [])
            if isinstance(lst, list):
                unknown = [c for c in lst if c not in cols]
                if unknown:
                    issues.append(self._warn(rname, rtype, f"{path}.{key}", f"Columns not in dataset hint: {unknown}"))

    @staticmethod
    def _is_number(v: Any) -> bool:
        try: float(v); return True
        except Exception: return False

    @staticmethod
    def _err(rule_name, rule_type, path, msg) -> ValidationIssue:
        return ValidationIssue(rule_name, rule_type, path, "ERROR", msg)

    @staticmethod
    def _warn(rule_name, rule_type, path, msg) -> ValidationIssue:
        return ValidationIssue(rule_name, rule_type, path, "WARN", msg)

    @staticmethod
    def _strip_json_comments_and_trailing_commas(s: str) -> str:
        s = re.sub(r"(?m)//.*?$", "", s)           # // line comments
        s = re.sub(r"/\*.*?\*/", "", s, flags=re.S) # /* */ blocks
        s = re.sub(r",\s*([}\]])", r"\1", s)        # trailing commas
        return s

from __future__ import annotations
import json, re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Callable

SUPPORTED_TYPES = ("headers","non_empty","range","enum","length","regex","unique","decimal")


class RuleSchemaValidationError(Exception):
    """Raised when rules.json schema validation fails in raise mode."""
    pass


@dataclass
class ValidationIssue:
    rule_name: str
    rule_type: str
    path: str
    level: str   # "ERROR" | "WARN"
    message: str


class RuleSchemaValidator:
    """
    Validates & normalizes rules.json structure.

    Returns:
        (is_valid: bool, rules: list, issues: list[ValidationIssue])

    Modes:
        collect  -> collect all issues, do not stop
        fail_fast -> stop on first ERROR, return immediately (no raise)
        raise    -> raise RuleSchemaValidationError on first ERROR
    """

    def __init__(self, default_decimal_precision: int = 18, default_decimal_scale: int = 2):
        self.default_precision = default_decimal_precision
        self.default_scale = default_decimal_scale

        self.validators: Dict[str, Callable] = {
            "headers":   self._validate_headers_rule,
            "non_empty": self._validate_non_empty_rule,
            "range":     self._validate_range_rule,
            "enum":      self._validate_enum_rule,
            "length":    self._validate_length_rule,
            "regex":     self._validate_regex_rule,
            "unique":    self._validate_unique_rule,
            "decimal":   self._validate_decimal_rule,
        }

    # -------------------- Public API --------------------

    def load(self, json_text: str) -> List[Dict[str, Any]]:
        rules = json.loads(json_text)
        if not isinstance(rules, list):
            raise ValueError("Rules JSON must be a list.")
        return rules

    def load_relaxed(self, json_text: str) -> List[Dict[str, Any]]:
        cleaned = self._strip_json_comments_and_trailing_commas(json_text)
        return self.load(cleaned)

    def validate(
        self,
        rules: List[Dict[str, Any]],
        dataset_columns: Optional[List[str]] = None,
        fail_on_warning: bool = False,
        mode: str = "collect"  # "collect" | "fail_fast" | "raise"
    ) -> Tuple[bool, List[Dict[str, Any]], List[ValidationIssue]]:
        """
        Returns: (is_valid: bool, rules, issues)
        """

        issues: List[ValidationIssue] = []
        names_seen = set()
        headers_count = 0

        for idx, rule in enumerate(rules):
            path = f"[{idx}]"
            rtype = str(rule.get("type","")).strip()
            rname = (str(rule.get("name","")).strip() or f"rule_{idx}")
            rule["name"] = rname

            # Unsupported rule type
            if rtype not in SUPPORTED_TYPES:
                issue = self._err(rname, rtype, path, f"Unsupported type '{rtype}'. Supported: {list(SUPPORTED_TYPES)}")
                if self._handle_issue(issue, issues, mode): 
                    return False, rules, issues
                continue

            # Duplicate rule name
            if rname in names_seen:
                issue = self._err(rname, rtype, path, "Duplicate rule name")
                if self._handle_issue(issue, issues, mode): 
                    return False, rules, issues
            names_seen.add(rname)

            if rtype == "headers":
                headers_count += 1

            # Dispatch to validator
            self.validators[rtype](rule, rname, rtype, path, dataset_columns, issues, mode)
            # If fail_fast triggered inside validator
            if mode == "fail_fast" and any(i.level == "ERROR" for i in issues):
                return False, rules, issues

        # Cross-rule advisories
        if headers_count == 0 and dataset_columns is not None:
            issue = self._warn("<schema>", "headers", "$", "No 'headers' rule present; column presence not enforced.")
            if self._handle_issue(issue, issues, mode):
                return False, rules, issues

        if headers_count > 1:
            issue = self._warn("<schema>", "headers", "$", "Multiple 'headers' rules present; consider consolidating.")
            if self._handle_issue(issue, issues, mode):
                return False, rules, issues

        # Promote WARN to ERROR if requested
        if fail_on_warning:
            for i in issues:
                if i.level == "WARN":
                    i.level = "ERROR"

        # Determine final boolean result
        is_valid = not any(i.level == "ERROR" for i in issues)
        return is_valid, rules, issues

    # -------------------- Per-Rule Validators --------------------

    def _validate_headers_rule(self, rule, rname, rtype, path, dataset_columns, issues, mode):
        self._require_list(rule, "columns", path, rname, rtype, issues, mode)
        if dataset_columns:
            missing = [c for c in rule.get("columns", []) if c not in dataset_columns]
            if missing:
                self._handle_issue(
                    self._warn(rname, rtype, f"{path}.columns", f"Columns not in dataset hint: {missing}"),
                    issues, mode
                )

    def _validate_non_empty_rule(self, rule, rname, rtype, path, dataset_columns, issues, mode):
        self._require_list(rule, "columns", path, rname, rtype, issues, mode)
        self._warn_unknown(rule, "columns", dataset_columns, rname, rtype, path, issues, mode)

    def _validate_range_rule(self, rule, rname, rtype, path, dataset_columns, issues, mode):
        self._require_keys(rule, ["column","min","max"], path, rname, rtype, issues, mode)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues, mode)
        if all(k in rule for k in ("min","max")):
            try:
                mn, mx = float(rule["min"]), float(rule["max"])
                if mn > mx:
                    self._handle_issue(self._err(rname, rtype, f"{path}.min/max", "min must be <= max"), issues, mode)
            except Exception:
                self._handle_issue(self._err(rname, rtype, f"{path}.min/max", "min/max must be numeric"), issues, mode)

    def _validate_enum_rule(self, rule, rname, rtype, path, dataset_columns, issues, mode):
        self._require_keys(rule, ["column"], path, rname, rtype, issues, mode)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues, mode)
        if "allowed" not in rule and "allowedValues" in rule:
            rule["allowed"] = rule["allowedValues"]
        allowed = rule.get("allowed")
        if not isinstance(allowed, list) or len(allowed) == 0:
            self._handle_issue(self._err(rname, rtype, f"{path}.allowed", "Provide non-empty 'allowed' list"), issues, mode)

    def _validate_length_rule(self, rule, rname, rtype, path, dataset_columns, issues, mode):
        self._require_keys(rule, ["column"], path, rname, rtype, issues, mode)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues, mode)
        mn = int(rule.get("min", 0)); mx = int(rule.get("max", 1_000_000))
        rule["min"], rule["max"] = mn, mx
        if mn < 0 or mx < 0 or mn > mx:
            self._handle_issue(self._err(rname, rtype, f"{path}.min/max", "0 ≤ min ≤ max required"), issues, mode)

    def _validate_regex_rule(self, rule, rname, rtype, path, dataset_columns, issues, mode):
        self._require_keys(rule, ["column","pattern"], path, rname, rtype, issues, mode)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues, mode)
        try:
            re.compile(str(rule.get("pattern","")))
        except re.error as e:
            self._handle_issue(self._err(rname, rtype, f"{path}.pattern", f"Invalid regex: {e}"), issues, mode)

    def _validate_unique_rule(self, rule, rname, rtype, path, dataset_columns, issues, mode):
        self._require_list(rule, "columns", path, rname, rtype, issues, mode)
        self._warn_unknown(rule, "columns", dataset_columns, rname, rtype, path, issues, mode)

    def _validate_decimal_rule(self, rule, rname, rtype, path, dataset_columns, issues, mode):
        self._require_keys(rule, ["column"], path, rname, rtype, issues, mode)
        self._warn_unknown(rule, "column", dataset_columns, rname, rtype, path, issues, mode)
        prec = int(rule.get("precision", self.default_precision))
        scale = int(rule.get("scale", self.default_scale))
        rule["precision"], rule["scale"] = prec, scale
        rule["exact_scale"] = bool(rule.get("exact_scale", False))
        if prec <= 0 or scale < 0 or scale > prec:
            self._handle_issue(self._err(rname, rtype, f"{path}.precision/scale", "Require precision>0 and 0≤scale≤precision"), issues, mode)

    # -------------------- Helper Methods --------------------

    def _handle_issue(self, issue: ValidationIssue, issues: List[ValidationIssue], mode: str) -> bool:
        issues.append(issue)

        if mode == "raise" and issue.level == "ERROR":
            raise RuleSchemaValidationError(f"[{issue.rule_type}:{issue.rule_name}] {issue.message} at {issue.path}")

        if mode == "fail_fast" and issue.level == "ERROR":
            return True  # stop further processing

        return False

    def _require_keys(self, obj, keys, path, rname, rtype, issues, mode):
        for k in keys:
            if k not in obj:
                self._handle_issue(self._err(rname, rtype, f"{path}.{k}", f"Missing required key '{k}'"), issues, mode)

    def _require_list(self, obj, key, path, rname, rtype, issues, mode):
        val = obj.get(key)
        ok = isinstance(val, list) and len(val) > 0 and all(isinstance(x, str) and x.strip() for x in val)
        if not ok:
            self._handle_issue(self._err(rname, rtype, f"{path}.{key}", f"Provide non-empty list of strings in '{key}'"), issues, mode)

    def _warn_unknown(self, obj, key, dataset_columns, rname, rtype, path, issues, mode):
        if not dataset_columns: return
        if key == "column":
            c = obj.get("column")
            if isinstance(c, str) and c not in dataset_columns:
                self._handle_issue(self._warn(rname, rtype, f"{path}.column", f"Column '{c}' not in dataset hint"), issues, mode)
        else:
            lst = obj.get(key, [])
            if isinstance(lst, list):
                unknown = [c for c in lst if c not in dataset_columns]
                if unknown:
                    self._handle_issue(self._warn(rname, rtype, f"{path}.{key}", f"Columns not in dataset hint: {unknown}"), issues, mode)

    @staticmethod
    def _err(rule_name, rule_type, path, msg) -> ValidationIssue:
        return ValidationIssue(rule_name, rule_type, path, "ERROR", msg)

    @staticmethod
    def _warn(rule_name, rule_type, path, msg) -> ValidationIssue:
        return ValidationIssue(rule_name, rule_type, path, "WARN", msg)

    @staticmethod
    def _strip_json_comments_and_trailing_commas(s: str) -> str:
        s = re.sub(r"(?m)//.*?$", "", s)
        s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)
        s = re.sub(r",\s*([}\]])", r"\1", s)
        return s

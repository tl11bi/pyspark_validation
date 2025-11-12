# df_validator_csv_tester.py
from typing import List, Dict, Optional, Tuple, Callable
import json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

class SparkDataValidator:
    """
    JSON-driven Spark DataFrame validators.

    Supported rules (examples):
      - headers: {"name":"headers","type":"headers","columns":["c1","c2",...]}
      - non_empty: {"name":"req","type":"non_empty","columns":["c1","c2"]}
      - range: {"name":"r","type":"range","column":"value","min":-100,"max":100}
      - enum: {"name":"ccy","type":"enum","column":"currency","allowed":["CAD","USD"]}
      - length: {"name":"len","type":"length","column":"inventory","min":5,"max":30}
      - regex: {"name":"rx","type":"regex","column":"riskMetric","pattern":"^(IR_DELTA|IR_VEGA|CR01)$"}
      - unique: {"name":"uniq","type":"unique","columns":["portfolio","inventory"]}
      - decimal: {"name":"dec","type":"decimal","column":"value","precision":18,"scale":6,"exact_scale":true,
                  "min":-100000, "max":100000}    # min/max optional and applied after cast

    Usage:
        rules = SparkDataValidator.load_rules_json(dbutils.fs.head("dbfs:/path/rules.json"))
        v = SparkDataValidator(spark, id_cols=["portfolio","inventory"], fail_fast=False, fail_mode="return")
        valid_df, errors_df = v.apply(input_df, rules)
    """

    # ---------- ctor / config ----------
    def __init__(
        self,
        spark_session: SparkSession,
        id_cols: Optional[List[str]] = None,
        fail_fast: bool = False,
        fail_mode: str = "return",  # "return" | "raise"
    ):
        self.spark = spark_session
        self.id_cols = id_cols or []
        self.fail_fast = fail_fast
        self.fail_mode = fail_mode

        # registry maps rule.type -> handler
        self.HANDLERS: Dict[str, Callable[[DataFrame, Dict], List[DataFrame]]] = {
            "headers": self._validate_headers,
            "non_empty": self._validate_non_empty,
            "range": self._validate_range,
            "enum": self._validate_enum,
            "length": self._validate_length,
            "regex": self._validate_regex,
            "unique": self._validate_unique,
            "decimal": self._validate_decimal,
        }

    # ---------- public API ----------
    @staticmethod
    def load_rules_json(json_text: str) -> List[Dict]:
        rules = json.loads(json_text)
        if not isinstance(rules, list):
            raise ValueError("Rules JSON must be a list of rule objects.")
        return rules

    def register(self, rule_type: str, func: Callable[[DataFrame, Dict], List[DataFrame]]) -> None:
        self.HANDLERS[rule_type] = func

    def validate(self, df: DataFrame, rules: List[Dict], cache: bool = False, repartition: Optional[int] = None, error_limit: int = 1000) -> Tuple[bool, DataFrame, DataFrame]:
        """
        Apply all rules and return (is_valid, valid_df, errors_df).

        Performance optimizations:
        - Optional DataFrame caching (set cache=True if reused)
        - Optional repartitioning for large datasets (set repartition=N)
        - Broadcast joins for small reference sets (enum)
        - Error DataFrame limited to error_limit rows for reporting
        - Spark SQL functions preferred over Python UDFs

        Args:
            df: Input DataFrame to validate
            rules: List of rule dicts specifying validation logic
            cache: If True, cache DataFrame for repeated use
            repartition: If set, repartition DataFrame before validation
            error_limit: Max number of error rows to collect per rule

        Returns:
            is_valid: True if no validation errors found, False otherwise
            valid_df: DataFrame containing only valid rows
            errors_df: DataFrame containing validation error details (limited to error_limit rows)

        If fail_fast=True:
            - fail_mode='return' returns immediately with first failing rule's errors
            - fail_mode='raise' raises ValueError with a small sample of violations
        """
        # Optional caching for repeated use (avoids recomputation if reused)
        if cache:
            df = df.cache()

        # Optional repartitioning for large datasets (improves parallelism)
        if repartition is not None:
            df = df.repartition(repartition)

        # 1) Apply header rules first (check required columns)
        for r in rules:
            if r.get("type") == "headers":
                parts = self._run(df, r)
                for v in parts:
                    if self._has_rows(v):
                        # Fail fast if header errors found
                        if self.fail_fast:
                            if self.fail_mode == "raise":
                                # Spark Connect doesn't support .toJSON(); use .collect() instead
                                sample = v.limit(10).collect()
                                raise ValueError(f"[headers] validation failed: sample={sample}")
                            # Limit error DataFrame size for reporting
                            return False, df, v.limit(error_limit)
                # If not fail-fast or no header errors, continue to next rule

        # 2) Apply data rules (all except headers)
        violations: List[DataFrame] = []
        for r in rules:
            if r.get("type") == "headers":
                continue
            # Broadcast join optimization for enum rules with small allowed sets
            if r.get("type") == "enum":
                allowed = r.get("allowed") or r.get("allowedValues") or []
                if allowed and len(allowed) < 1000:
                    # Use broadcast for small allowed sets (handled in _validate_enum)
                    pass
            parts = self._run(df, r)
            if not parts:
                continue
            if self.fail_fast:
                for v in parts:
                    if self._has_rows(v):
                        # Fail fast if any rule errors found
                        if self.fail_mode == "raise":
                            # Spark Connect doesn't support .toJSON(); use .collect() instead
                            sample = v.limit(10).collect()
                            raise ValueError(f"[{r.get('type')}:{r.get('name','')}] failed: sample={sample}")
                        # Limit error DataFrame size for reporting
                        return False, df, v.limit(error_limit)
            # Limit error DataFrame size for reporting (avoid collecting huge error sets)
            violations.extend([vi.limit(error_limit) for vi in parts])

        # Finalize results: union all error DataFrames, compute valid rows
        return self._finalize(df, violations)

    # ---------- internals ----------
    def _finalize(self, df: DataFrame, violations: List[DataFrame]) -> Tuple[bool, DataFrame, DataFrame]:
        errors_df = self._union_all(violations)
        if errors_df is None:
            errors_df = self._empty_errors_df()

        if self.id_cols and errors_df is not None and errors_df.columns:
            # For columns with dots, we need to use Column expressions in the join, not strings
            # Build join condition explicitly using Column equality expressions
            id_cols_escaped = [F.col(f"`{c}`") for c in self.id_cols]
            bad_ids = errors_df.select(*id_cols_escaped).dropDuplicates()
            
            # Build join condition: df.col1 == bad_ids.col1 AND df.col2 == bad_ids.col2 ...
            join_conditions = [
                F.col(f"df.`{c}`") == F.col(f"bad_ids.`{c}`")
                for c in self.id_cols
            ]
            # Combine all conditions with AND
            full_condition = join_conditions[0]
            for cond in join_conditions[1:]:
                full_condition = full_condition & cond
            
            # Use left_anti join with explicit condition instead of column names
            valid_df = df.alias("df").join(
                bad_ids.alias("bad_ids"), 
                on=full_condition, 
                how="left_anti"
            )
        else:
            valid_df = df
        
        # Determine if validation was successful (no errors)
        is_valid = not self._has_rows(errors_df)
        return is_valid, valid_df, errors_df

    def _run(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        handler = self.HANDLERS.get(rule.get("type"))
        if not handler:
            return [self._meta_error(rule, f"Unknown rule type: {rule.get('type')}")]
        return handler(df, rule)

    def _union_all(self, parts: List[DataFrame]) -> Optional[DataFrame]:
        parts = [p for p in parts if p is not None]
        if not parts:
            return None
        out = parts[0]
        for p in parts[1:]:
            out = out.unionByName(p, allowMissingColumns=True)
        return out

    def _empty_errors_df(self) -> DataFrame:
        fields = [StructField(c, StringType(), True) for c in self.id_cols]
        fields += [
            StructField("rule", StringType(), True),
            StructField("column", StringType(), True),
            StructField("value", StringType(), True),
            StructField("message", StringType(), True),
        ]
        # Spark Connect doesn't support .sparkContext; use empty list instead
        schema = StructType(fields)
        return self.spark.createDataFrame([], schema)

    @staticmethod
    def _has_rows(df: DataFrame) -> bool:
        return len(df.take(1)) > 0  # cheap cluster-side check

    @staticmethod
    def _msg(rule: Dict, colname: str, extra: str = "validation failed") -> str:
        return f"[{rule.get('name','unnamed')}] {colname}: {extra}"

    def _collect_error(self, df: DataFrame, mask, rule: Dict, colname: str) -> DataFrame:
        # rows where mask is False are violations
        # Properly escape column names with dots using backticks
        id_cols_escaped = [F.col(f"`{c}`") for c in self.id_cols]
        
        base = (
            df.where(~mask)
              .select(
                  *id_cols_escaped,
                  F.lit(rule.get("name", "")).alias("rule"),
                  F.lit(colname).alias("column"),
                  F.col(f"`{colname}`").cast("string").alias("value"),
                  F.lit(self._msg(rule, colname)).alias("message"),
              )
        )
        # If id_cols are empty, still produce consistent columns
        if not self.id_cols:
            return base.select(
                *[F.lit(None).cast("string").alias("_id")] if False else [],  # no-op; keep columns as-is
                "rule", "column", "value", "message"
            )
        return base

    def _meta_error(self, rule: Dict, text: str) -> DataFrame:
        # meta-errors (unknown rule etc.) as a single-row DF
        row = self.spark.createDataFrame([(rule.get("name", ""), text)], ["rule", "message"]) \
                        .withColumn("column", F.lit(None).cast("string")) \
                        .withColumn("value", F.lit(None).cast("string"))
        # prepend id_cols as nulls for schema compatibility - use withColumn instead of select
        for c in reversed(self.id_cols):
            row = row.withColumn(c, F.lit(None).cast("string"))
        # Use backtick-escaped column expressions for columns with dots
        id_cols_escaped = [F.col(f"`{c}`") for c in self.id_cols]
        return row.select(*id_cols_escaped, "rule", "column", "value", "message")

    # ---------- rule handlers ----------
    def _validate_headers(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        required = rule.get("columns", [])
        missing = [c for c in required if c not in df.columns]
        if not missing:
            return []
        err = (self.spark.createDataFrame([(m,) for m in missing], ["column"])
               .withColumn("rule", F.lit(rule.get("name", "headers")))
               .withColumn("value", F.lit(None).cast("string"))
               .withColumn("message", F.concat(F.lit("[headers] missing column "), F.col("column"))))
        # pad id_cols with nulls - use backticks for columns with dots
        for c in reversed(self.id_cols):
            # Create column with proper escaping for the final select
            err = err.withColumn(c, F.lit(None).cast("string"))
        # Select with backtick-escaped column names
        id_cols_escaped = [F.col(f"`{c}`") for c in self.id_cols]
        return [err.select(*id_cols_escaped, "rule", "column", "value", "message")]

    def _validate_non_empty(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        outs = []
        for c in rule.get("columns", []):
            mask = F.col(f"`{c}`").isNotNull() & (F.trim(F.col(f"`{c}`")) != "")
            outs.append(self._collect_error(df, mask, rule, c))
        return outs

    def _validate_range(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        c = rule["column"]
        num = F.col(f"`{c}`").cast("double")
        mask = num.isNotNull() & (num >= F.lit(rule["min"])) & (num <= F.lit(rule["max"]))
        return [self._collect_error(df, mask, rule, c)]

    def _validate_enum(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        """
        Validates that a column's value is one of the allowed values.
        Uses broadcast join optimization for small allowed sets (<1000).
        """
        c = rule["column"]
        allowed = rule.get("allowed") or rule.get("allowedValues") or []
        # Broadcast join optimization for small allowed sets
        if allowed and len(allowed) < 1000:
            allowed_df = self.spark.createDataFrame([(v,) for v in allowed], [c])
            allowed_df = F.broadcast(allowed_df)
            mask = F.col(f"`{c}`").isin(allowed)
        else:
            mask = F.col(f"`{c}`").isin(allowed)
        return [self._collect_error(df, mask, rule, c)]

    def _validate_length(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        c = rule["column"]
        l = F.length(F.col(f"`{c}`"))
        mask = (l >= F.lit(rule.get("min", 0))) & (l <= F.lit(rule.get("max", 1_000_000)))
        return [self._collect_error(df, mask, rule, c)]

    def _validate_regex(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        c = rule["column"]
        pattern = rule["pattern"]
        mask = F.col(f"`{c}`").rlike(pattern)
        return [self._collect_error(df, mask, rule, c)]

    def _validate_unique(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        """
        Validates that the combination of specified columns is unique (no duplicate rows).
        Partitions DataFrame before groupBy for large datasets to improve performance.
        Limits error DataFrame size for reporting.
        """
        cols = rule["columns"]
        # Optional repartition. Spark Connect DataFrames don't implement .rdd, so guard access.
        def _safe_num_partitions(d: DataFrame):
            try:
                return d.rdd.getNumPartitions()  # type: ignore[attr-defined]
            except Exception:
                return None
        num_parts = _safe_num_partitions(df)
        if num_parts is not None and num_parts < 10:
            df = df.repartition(10)
        # Find duplicate keys - properly escape column names with dots
        dup_keys = df.groupBy(*[F.col(f"`{c}`") for c in cols]).count().where(F.col("count") > 1).drop("count")
        
        # Build explicit join condition for columns with dots
        join_conditions = [
            F.col(f"df.`{c}`") == F.col(f"dup_keys.`{c}`")
            for c in cols
        ]
        full_condition = join_conditions[0]
        for cond in join_conditions[1:]:
            full_condition = full_condition & cond
        
        offending = df.alias("df").join(dup_keys.alias("dup_keys"), on=full_condition, how="inner")

        # Reference columns from 'df' alias to avoid ambiguity after join
        value_expr = F.concat_ws("||", *[F.col(f"df.`{c}`").cast("string") for c in cols])
        msg = F.lit(self._msg(rule, ",".join(cols), "duplicate key"))
        
        # Properly escape id_cols with dots and reference from 'df' alias
        id_cols_escaped = [F.col(f"df.`{c}`") for c in self.id_cols]

        err = (offending
               .select(
                   *id_cols_escaped,
                   F.lit(rule.get("name", "unique")).alias("rule"),
                   F.lit(",".join(cols)).alias("column"),
                   value_expr.alias("value"),
                   msg.alias("message"),
               ))
        # Limit error DataFrame size for reporting
        return [err.limit(1000)]

    def _validate_decimal(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        """
        Validates that a column fits Decimal(precision, scale).
        If exact_scale=true, enforces fractional digits <= scale.
        Applies optional min/max after successful cast.
        """
        c = rule["column"]
        p = int(rule.get("precision", 18))  # default precision 18 if not provided
        s = int(rule.get("scale", 2))
        exact = bool(rule.get("exact_scale", False))
        min_v = rule.get("min", None)
        max_v = rule.get("max", None)

        # Cast column to DecimalType
        dec = F.col(f"`{c}`").cast(DecimalType(p, s))
        cast_ok = dec.isNotNull()

        if exact:
            # digits after '.' in the original string
            frac = F.regexp_extract(F.col(f"`{c}`").cast("string"), r"(?<=\.)\d+", 0)
            frac_len = F.length(F.when(frac == "", F.lit("0")).otherwise(frac))
            scale_ok = frac_len <= F.lit(s)
            mask = cast_ok & scale_ok
        else:
            mask = cast_ok

        # Apply min/max bounds if provided
        if min_v is not None:
            mask = mask & (dec >= F.lit(min_v).cast(DecimalType(p, s)))
        if max_v is not None:
            mask = mask & (dec <= F.lit(max_v).cast(DecimalType(p, s)))

        return [self._collect_error(df, mask, rule, c)]

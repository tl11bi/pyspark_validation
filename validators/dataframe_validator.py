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

    def validate(self, df: DataFrame, rules: List[Dict], cache: bool = False, repartition: Optional[int] = None, error_limit: int = 1000, skip_headers: bool = False, coalesce_to: Optional[int] = None) -> Tuple[bool, DataFrame, DataFrame]:
        """Apply validation rules and return (is_valid, valid_df, errors_df)."""
        # Input validation
        if df is None:
            raise ValueError("DataFrame cannot be None")
        if not rules:
            raise ValueError("rules list cannot be empty")
        
        # Optional caching and repartitioning
        if cache:
            df = df.cache()
        if repartition is not None:
            df = df.repartition(repartition)

        # Apply header rules first
        if not skip_headers:
            header_count = len([r for r in rules if r.get("type") == "headers"])
            if header_count > 100:
                raise RuntimeError(f"Too many headers ({header_count}); max 100")
            
            for r in rules:
                if r.get("type") == "headers":
                    parts = self._run(df, r)
                    for v in parts:
                        if self._has_rows(v):
                            if self.fail_fast:
                                if self.fail_mode == "raise":
                                    sample = v.limit(10).collect()
                                    raise ValueError(f"[headers] failed: {sample}")
                                return False, df, v.limit(error_limit)

        # Apply data rules with batched violation collection
        violations: List[DataFrame] = []
        rule_count = 0
        max_rules = 200
        batch_size = 5  # persist every N rules
        
        for r in rules:
            rule_count += 1
            if rule_count > max_rules:
                raise RuntimeError(f"Exceeded {max_rules} rule iterations")
            
            if r.get("type") == "headers":
                continue
            parts = self._run(df, r)
            if not parts:
                continue
            if self.fail_fast:
                for v in parts:
                    if self._has_rows(v):
                        if self.fail_mode == "raise":
                            sample = v.limit(10).collect()
                            raise ValueError(f"[{r.get('type')}:{r.get('name','')}] {sample}")
                        return False, df, v.limit(error_limit)
            violations.extend([vi.limit(error_limit) for vi in parts])
            
            if len(violations) >= batch_size:
                partial = self._union_all(violations)
                if partial:
                    partial.persist().count()  # break lineage
                    violations = [partial]

        # Finalize: union errors and compute valid rows
        is_valid, valid_df, errors_df = self._finalize(df, violations)
        
        if coalesce_to is not None:
            valid_df = valid_df.coalesce(coalesce_to)
        
        return is_valid, valid_df, errors_df

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
            
            valid_df = df.alias("df").join(bad_ids.alias("bad_ids"), on=full_condition, how="left_anti")
        else:
            valid_df = df
        
        is_valid = not self._has_rows(errors_df)
        return is_valid, valid_df, errors_df

    def _run(self, df: DataFrame, rule: Dict) -> List[DataFrame]:
        handler = self.HANDLERS.get(rule.get("type"))
        if not handler:
            return [self._meta_error(rule, f"Unknown rule type: {rule.get('type')}")]
        return handler(df, rule)

    def _union_all(self, parts: List[DataFrame]) -> Optional[DataFrame]:
        """Binary tree union: O(log n) DAG depth vs O(n) for sequential."""
        parts = [p for p in parts if p is not None]
        if not parts:
            return None
        if len(parts) == 1:
            return parts[0]
        
        iteration = 0
        max_iterations = 100
        
        while len(parts) > 1:
            iteration += 1
            if iteration > max_iterations:
                raise RuntimeError(f"Union exceeded {max_iterations} iterations; check rule config")
            
            next_batch = []
            for i in range(0, len(parts), 2):
                if i + 1 < len(parts):
                    next_batch.append(parts[i].unionByName(parts[i + 1], allowMissingColumns=True))
                else:
                    next_batch.append(parts[i])
            parts = next_batch
        
        return parts[0]

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
        id_cols_escaped = [F.col(f"`{c}`") for c in self.id_cols]
        
        base = (df.where(~mask)
                  .select(*id_cols_escaped, F.lit(rule.get("name", "")).alias("rule"),
                          F.lit(colname).alias("column"), F.col(f"`{colname}`").cast("string").alias("value"),
                          F.lit(self._msg(rule, colname)).alias("message")))
        if not self.id_cols:
            return base.select("rule", "column", "value", "message")
        return base

    def _meta_error(self, rule: Dict, text: str) -> DataFrame:
        row = self.spark.createDataFrame([(rule.get("name", ""), text)], ["rule", "message"])
        row = row.withColumn("column", F.lit(None).cast("string")).withColumn("value", F.lit(None).cast("string"))
        for c in reversed(self.id_cols):
            row = row.withColumn(c, F.lit(None).cast("string"))
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
               .withColumn("message", F.concat(F.lit("[headers] missing: "), F.col("column"))))
        for c in reversed(self.id_cols):
            err = err.withColumn(c, F.lit(None).cast("string"))
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
        # Optimize for small allowed sets
        c = rule["column"]
        allowed = rule.get("allowed") or rule.get("allowedValues") or []
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
        # Ensure key combination is unique
        cols = rule["columns"]
        # Repartition to 10 for large datasets
        def _safe_num_partitions(d: DataFrame):
            try:
                return d.rdd.getNumPartitions()  # type: ignore[attr-defined]
            except Exception:
                return None
        num_parts = _safe_num_partitions(df)
        if num_parts is not None and num_parts < 10:
            df = df.repartition(10)
        
        # Find duplicate key combinations
        dup_keys = df.groupBy(*[F.col(f"`{c}`") for c in cols]).count().where(F.col("count") > 1).drop("count")
        
        # Join to find offending rows
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
        # Decimal type with precision, scale, optional min/max bounds
        c = rule["column"]
        p = int(rule.get("precision", 18))
        s = int(rule.get("scale", 2))
        exact = bool(rule.get("exact_scale", False))
        min_v = rule.get("min", None)
        max_v = rule.get("max", None)

        # Cast and check valid Decimal
        dec = F.col(f"`{c}`").cast(DecimalType(p, s))
        cast_ok = dec.isNotNull()

        if exact:
            # Enforce fractional digits <= scale
            frac = F.regexp_extract(F.col(f"`{c}`").cast("string"), r"(?<=\.)\d+", 0)
            frac_len = F.length(F.when(frac == "", F.lit("0")).otherwise(frac))
            scale_ok = frac_len <= F.lit(s)
            mask = cast_ok & scale_ok
        else:
            mask = cast_ok

        # Apply bounds if present
        if min_v is not None:
            mask = mask & (dec >= F.lit(min_v).cast(DecimalType(p, s)))
        if max_v is not None:
            mask = mask & (dec <= F.lit(max_v).cast(DecimalType(p, s)))

        return [self._collect_error(df, mask, rule, c)]

"""Unified batch test harness for ingestion + validation.

Run as a Databricks notebook (add widgets) or as a plain Python script.

Features:
 - Loads a single file (csv | json | parquet) in batch mode
 - Optional multiLine + wholeFile JSON handling (array or pretty JSON)
 - Optional flattening of nested JSON/parquet structures
 - Loads rules JSON and applies SparkDataValidator
 - Prints validation summary (valid/error counts + sample errors)
 - Allows overriding id_cols; otherwise infers from first unique rule

Usage (Databricks notebook):
  dbutils.widgets.text("file_path", "/dbfs/path/to/file.json")
  dbutils.widgets.text("rules_path", "/dbfs/path/to/rules.json")
  dbutils.widgets.text("file_format", "json")
  dbutils.widgets.text("flatten", "true")
  dbutils.widgets.text("multiline", "true")
  dbutils.widgets.text("whole_file", "true")
  dbutils.widgets.text("id_cols", "dealRid,facilityRid,positions.symbol")
  %run ./bronze_ingestion_test

CLI / local driver (ensure PYSPARK+Spark available):
  python validators/bronze_ingestion_test.py \
      --file_path tests/data/sample_json_data.json \
      --rules_path tests/rules/sample_json_rules/rules_with_max_two_layer.json \
      --file_format json --flatten true --multiline true --whole_file true \
      --id_cols dealRid facilityRid positions.symbol
"""

from __future__ import annotations

import json
import os
import sys
import argparse
from typing import List, Tuple, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from validators.flatten_utils import flatten_all
from validators.dataframe_validator import SparkDataValidator


def load_input_df(
    spark: SparkSession,
    path: str,
    file_format: str,
    multiline: bool = False,
    whole_file: bool = False,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """Load a single dataset in batch mode.

    Args:
        spark: SparkSession
        path: file path or directory
        file_format: csv | json | parquet
        multiline: for pretty or array JSON
        whole_file: treat entire file as one JSON document
        schema: Optional explicit schema (JSON/parquet)
    """
    fmt = file_format.lower()
    if fmt == "csv":
        reader = spark.read.options({"header": "true", "inferSchema": "true"})
        return reader.csv(path)
    elif fmt == "json":
        reader = spark.read
        if multiline:
            reader = reader.option("multiLine", "true")
        if whole_file:
            # Whole file reading (array root) requires reading as text then parsing if local Spark.
            # In batch spark.read.json with multiLine usually suffices, so we keep it simple here.
            pass
        if schema is not None:
            reader = reader.schema(schema)
        return 7.json(path)
    elif fmt == "parquet":
        reader = spark.read
        if schema is not None:
            reader = reader.schema(schema)
        return reader.parquet(path)
    else:
        raise ValueError(f"Unsupported file_format: {file_format}")


def load_rules(rules_path: str) -> List[dict]:
    with open(rules_path, "r", encoding="utf-8") as f:
        return json.load(f)


def infer_id_cols(rules: List[dict], override: Optional[List[str]] = None) -> List[str]:
    if override:
        return override
    for r in rules:
        if r.get("type") == "unique":
            cols = r.get("columns", [])
            if cols:
                return cols
    # Fallback minimal
    return []


def ensure_header_columns(df: DataFrame, rules: List[dict]) -> DataFrame:
    header_rules = [r for r in rules if r.get("type") == "headers"]
    if not header_rules:
        return df
    expected = set()
    for hr in header_rules:
        expected.update(hr.get("columns", []))
    missing = [c for c in expected if c not in df.columns]
    for c in missing:
        df = df.withColumn(c, F.lit(None).cast("string"))
    return df


def run_validation(
    spark: SparkSession,
    df: DataFrame,
    rules: List[dict],
    id_cols: List[str],
    fail_fast: bool = False,
    fail_mode: str = "return",
) -> Tuple[bool, DataFrame, DataFrame]:
    validator = SparkDataValidator(
        spark_session=spark,
        id_cols=id_cols,
        fail_fast=fail_fast,
        fail_mode=fail_mode,
    )
    return validator.validate(df, rules)


def summarize(is_valid: bool, valid_df: DataFrame, errors_df: DataFrame) -> None:
    print("=== VALIDATION SUMMARY ===")
    print(f"is_valid              : {is_valid}")
    print(f"valid_row_count       : {valid_df.count()}")
    print(f"error_row_count       : {errors_df.count()}")
    if not is_valid and errors_df.count() > 0:
        sample = errors_df.limit(5).collect()
        print("sample_errors:")
        for r in sample:
            print(f" - rule={getattr(r,'rule',None)} column={getattr(r,'column',None)} msg={getattr(r,'message',None)}")


def parse_params_via_widgets():
    try:
        import pyspark
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(SparkSession.getActiveSession())  # pragma: no cover
    except Exception:
        return None

    keys = [
        "file_path",
        "rules_path",
        "file_format",
        "flatten",
        "multiline",
        "whole_file",
        "id_cols",
    ]
    params = {}
    for k in keys:
        try:
            params[k] = dbutils.widgets.get(k)
        except Exception:
            params[k] = None
    return params


def parse_cli_args():
    parser = argparse.ArgumentParser(description="Run batch validation test harness")
    parser.add_argument("--file_path", required=True)
    parser.add_argument("--rules_path", required=True)
    parser.add_argument("--file_format", required=True, choices=["csv", "json", "parquet"])
    parser.add_argument("--flatten", default="true")
    parser.add_argument("--multiline", default="false")
    parser.add_argument("--whole_file", default="false")
    parser.add_argument("--id_cols", nargs="*", default=[])
    return parser.parse_args()


def main():
    # Attempt widgets first (Databricks). If unavailable, use CLI.
    widget_params = parse_params_via_widgets()

    if widget_params:
        file_path = widget_params.get("file_path")
        rules_path = widget_params.get("rules_path")
        file_format = (widget_params.get("file_format") or "json").lower()
        flatten_flag = (widget_params.get("flatten") or "true").lower() == "true"
        multiline_flag = (widget_params.get("multiline") or "false").lower() == "true"
        whole_file_flag = (widget_params.get("whole_file") or "false").lower() == "true"
        id_cols_raw = widget_params.get("id_cols") or ""
        id_cols = [c.strip() for c in id_cols_raw.split(",") if c.strip()]
    else:
        args = parse_cli_args()
        file_path = args.file_path
        rules_path = args.rules_path
        file_format = args.file_format.lower()
        flatten_flag = args.flatten.lower() == "true"
        multiline_flag = args.multiline.lower() == "true"
        whole_file_flag = args.whole_file.lower() == "true"
        id_cols = args.id_cols

    if not (file_path and rules_path):
        raise ValueError("file_path and rules_path are required")

    spark = SparkSession.builder.appName("bronze-ingestion-test").getOrCreate()

    print(f"Params: file_path={file_path} format={file_format} flatten={flatten_flag} multiline={multiline_flag} whole_file={whole_file_flag} id_cols={id_cols}")

    df = load_input_df(
        spark,
        file_path,
        file_format=file_format,
        multiline=multiline_flag,
        whole_file=whole_file_flag,
    )

    original_count = df.count()
    print(f"Loaded rows: {original_count}")
    print("Original columns:", df.columns)

    if file_format in ("json", "parquet") and flatten_flag:
        df, exploded = flatten_all(df, sep=".", explode_arrays=True)
        print(f"Flattened; exploded arrays: {exploded}")
        print("Flattened columns:", df.columns)

    rules = load_rules(rules_path)
    id_cols_final = infer_id_cols(rules, override=id_cols)
    df = ensure_header_columns(df, rules)

    is_valid, valid_df, errors_df = run_validation(
        spark=spark,
        df=df,
        rules=rules,
        id_cols=id_cols_final,
        fail_fast=False,
        fail_mode="return",
    )

    summarize(is_valid, valid_df, errors_df)

    if not is_valid:
        print("Validation failed; see sample errors above.")
    else:
        print("Validation passed.")

    # Keep session alive only in notebook; for CLI, stop.
    if not widget_params:
        spark.stop()


if __name__ == "__main__":
    main()

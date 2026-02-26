"""
Utilities for flattening nested PySpark DataFrames with struct and array columns.

Optimized for Spark SQL to minimize DataFrame transformations and leverage
lazy evaluation for better query plan optimization.
"""
from typing import List, Set, Optional
import json
import warnings
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F, types as T



def flatten_by_header_list(
    df: DataFrame,
    headers_list: List[str],
    sep: str = ".",
    explode_arrays: bool = True,
    break_lineage_every: int = 0,
    use_checkpoint: bool = False,
    repartition_to: Optional[int] = None,
) -> DataFrame:
    """Flatten and optionally explode arrays in specified column paths."""
    # Input validation
    if df is None:
        raise ValueError("DataFrame cannot be None")
    if not headers_list:
        raise ValueError("headers_list cannot be empty")
    if break_lineage_every < 0:
        raise ValueError("break_lineage_every must be >= 0")
    if repartition_to is not None and repartition_to <= 0:
        raise ValueError("repartition_to must be > 0")
    if use_checkpoint and break_lineage_every == 0:
        warnings.warn(
            "use_checkpoint=True but break_lineage_every=0",
            RuntimeWarning,
        )
    
    # Verify checkpoint dir configured
    if use_checkpoint and break_lineage_every > 0:
        try:
            checkpoint_dir = df.sparkSession.sparkContext.getCheckpointDir()
            if not checkpoint_dir:
                raise RuntimeError(
                    "Checkpoint dir not set. "
                    "Call: spark.sparkContext.setCheckpointDir('/path')"
                )
        except Exception as e:
            raise RuntimeError(f"Checkpoint config failed: {e}")
    
    if repartition_to is not None:
        df = df.repartition(repartition_to)

    seen = set()
    paths: List[str] = []
    for path in headers_list:
        if path not in seen:
            paths.append(path)
            seen.add(path)

    # Cache schema to avoid repeated walks
    schema = df.schema

    arrays_to_explode = _collect_required_arrays(schema, paths, sep)
    arrays_to_explode.sort(key=lambda p: p.count(sep))
    if arrays_to_explode and not explode_arrays:
        warnings.warn(
            "Arrays detected in rule paths; explode_arrays=False may produce arrays in output.",
            RuntimeWarning,
        )

    if explode_arrays and arrays_to_explode:
        for index, arr_path in enumerate(arrays_to_explode, start=1):
            try:
                df = df.withColumn(arr_path, F.explode_outer(F.col(f"`{arr_path}`")))
                # Flatten structs produced by explode
                df = _flatten_structs_optimized(df, sep=sep)
                schema = df.schema  # Update schema cache
                if break_lineage_every and index % break_lineage_every == 0:
                    df = _break_lineage(df, use_checkpoint)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to explode array '{arr_path}': {e}"
                )

    # Select requested columns, skip missing ones
    select_cols = []
    missing_paths = []
    for path in paths:
        if _path_exists(schema, path, sep):
            select_cols.append(F.col(f"`{path}`").alias(path))
        else:
            missing_paths.append(path)

    if missing_paths:
        warnings.warn(
            f"Paths not found (skipped): {missing_paths}",
            RuntimeWarning,
        )

    return df.select(*select_cols) if select_cols else df


def flatten_by_rules_json(
    df: DataFrame,
    rules_text: str,
    sep: str = ".",
    explode_arrays: bool = True,
    break_lineage_every: int = 0,
    use_checkpoint: bool = False,
    repartition_to: Optional[int] = None,
) -> DataFrame:
    """Extract columns from 'type: headers' rules and flatten."""
    try:
        rules = json.loads(rules_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in rules_text: {e}")
    
    if not isinstance(rules, list):
        raise ValueError("rules must be a JSON list (array of rule objects)")
    
    headers_list = extract_pathes_from_rule(rules)
    if not headers_list:
        raise ValueError("No headers rules found in rules JSON. Ensure 'type': 'headers' exists.")
    
    return flatten_by_header_list(
        df,
        headers_list,
        sep=sep,
        explode_arrays=explode_arrays,
        break_lineage_every=break_lineage_every,
        use_checkpoint=use_checkpoint,
        repartition_to=repartition_to,
    )


def extract_pathes_from_rule(rules: List[dict]) -> List[str]:
    """
    Extracts all column paths from headers rules only.
    """
    paths: List[str] = []
    for rule in rules:
        rtype = rule.get("type")
        if rtype == "headers":
            paths.extend(rule.get("columns", []))
    return paths


def _collect_required_arrays(schema: T.StructType, paths: List[str], sep: str, max_depth: int = 100) -> List[str]:
    arrays: List[str] = []
    seen: Set[str] = set()

    for path in paths:
        parts = path.split(sep)
        if len(parts) > max_depth:
            raise ValueError(f"Path depth exceeds max_depth ({max_depth}): {path}")
        
        current_type: T.DataType = schema
        path_so_far: List[str] = []

        for part in parts:
            if isinstance(current_type, T.StructType):
                field = next((f for f in current_type.fields if f.name == part), None)
                if field is None:
                    break
                path_so_far.append(part)
                dtype = field.dataType
                if isinstance(dtype, T.ArrayType):
                    arr_path = sep.join(path_so_far)
                    if arr_path not in seen:
                        arrays.append(arr_path)
                        seen.add(arr_path)
                    current_type = dtype.elementType
                else:
                    current_type = dtype
            elif isinstance(current_type, T.ArrayType):
                current_type = current_type.elementType
            else:
                break

    return arrays


def _path_exists(schema: T.StructType, path: str, sep: str, max_depth: int = 100) -> bool:
    if any(f.name == path for f in schema.fields):
        return True

    parts = path.split(sep)
    if len(parts) > max_depth:
        raise ValueError(f"Path depth exceeds max_depth ({max_depth}): {path}")
    
    current_type: T.DataType = schema

    for part in parts:
        if isinstance(current_type, T.StructType):
            field = next((f for f in current_type.fields if f.name == part), None)
            if field is None:
                return False
            current_type = field.dataType
        elif isinstance(current_type, T.ArrayType):
            current_type = current_type.elementType
        else:
            return False

    return True


def _break_lineage(df: DataFrame, use_checkpoint: bool) -> DataFrame:
    if use_checkpoint:
        return df.checkpoint(eager=True)
    df = df.persist()
    df.count()
    return df


def _flatten_structs_optimized(df: DataFrame, sep: str = ".", max_depth: int = 100) -> DataFrame:
    """Recursively flatten nested struct columns."""
    out = df
    
    for depth in range(max_depth):
        schema_fields = out.schema.fields
        
        # Exit if no structs remain
        has_structs = any(isinstance(f.dataType, T.StructType) for f in schema_fields)
        if not has_structs:
            return out
        
        # Expand one level of struct columns
        cols = []
        for field in schema_fields:
            if isinstance(field.dataType, T.StructType):
                for nested_field in field.dataType.fields:
                    col_expr = F.col(f"`{field.name}`.`{nested_field.name}`")
                    alias_name = f"{field.name}{sep}{nested_field.name}"
                    cols.append(col_expr.alias(alias_name))
            else:
                cols.append(F.col(f"`{field.name}`"))
        
        out = out.select(*cols)
    
    raise RuntimeError(f"Max depth ({max_depth}) exceeded")

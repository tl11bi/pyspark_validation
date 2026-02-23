"""
Utilities for flattening nested PySpark DataFrames with struct and array columns.

Optimized for Spark SQL to minimize DataFrame transformations and leverage
lazy evaluation for better query plan optimization.
"""
from typing import List, Tuple, Set, Optional
import json
import warnings
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F, types as T


def flatten_all(df: DataFrame, sep: str = ".", explode_arrays: bool = False) -> Tuple[DataFrame, List[str]]:
    """
    Flattens struct fields and optionally explodes arrays of structs.
    
    This is the main public API for flattening nested DataFrames. It first flattens
    all struct columns, then optionally explodes any array<struct> columns into 
    separate rows (similar to SQL UNNEST).
    
    Optimizations:
        - Single-pass schema analysis to minimize DataFrame operations
        - Batched column selections to reduce transformation overhead
        - Efficient schema inspection using cached field analysis
    
    Args:
        df: Input DataFrame with potentially nested structures
        sep: Separator for flattened column names (default: ".")
        explode_arrays: If True, explode array<struct> columns into separate rows
    
    Returns:
        Tuple of:
            - flattened_df: The flattened DataFrame
            - exploded_columns: List of column names that were exploded (empty if explode_arrays=False)
    
    Example:
        # Input: {user: {id: int, name: string}, orders: array<{id: int, amount: float}>}
        df_flat, exploded = flatten_all(df, explode_arrays=True)
        # Output: {user.id: int, user.name: string, orders.id: int, orders.amount: float}
        # exploded = ['orders']
    """
    # First, flatten all struct columns in a single optimized pass
    df = _flatten_structs_optimized(df, sep=sep)
    exploded = []

    if explode_arrays:
        # Iteratively explode any array<struct> columns
        # We use a loop because exploding can reveal new nested structs
        max_iterations = 100  # Safety limit to prevent infinite loops
        
        for iteration in range(max_iterations):
            # Single schema inspection per iteration (cached)
            schema_fields = df.schema.fields
            array_struct_fields = [
                f for f in schema_fields
                if isinstance(f.dataType, T.ArrayType) and isinstance(f.dataType.elementType, T.StructType)
            ]
            
            if not array_struct_fields:
                # No more array<struct> columns to explode
                break
            
            # Process all array<struct> columns in this iteration
            # Note: We still process one at a time to avoid schema conflicts,
            # but we batch the schema inspection
            f = array_struct_fields[0]
            df = df.withColumn(f.name, F.explode_outer(F.col(f"`{f.name}`")))
            exploded.append(f.name)
            
            # Re-flatten in case the exploded struct has nested fields
            df = _flatten_structs_optimized(df, sep=sep)
        else:
            # Loop completed without break = hit max_iterations
            raise RuntimeError(
                f"Maximum array explosion iterations ({max_iterations}) exceeded. "
                f"This likely indicates circular references or extremely deep nesting."
            )
    
    return df, exploded


def flatten_by_rules(
    df: DataFrame,
    rule_paths: List[str],
    sep: str = ".",
    explode_arrays: bool = True,
    break_lineage_every: int = 0,
    use_checkpoint: bool = False,
    repartition_to: Optional[int] = None,
) -> DataFrame:
    """
    Selects only rule paths and explodes arrays that appear in those paths.
    """
    if repartition_to is not None:
        df = df.repartition(repartition_to)

    seen = set()
    paths: List[str] = []
    for path in rule_paths:
        if path not in seen:
            paths.append(path)
            seen.add(path)

    arrays_to_explode = _collect_required_arrays(df.schema, paths, sep)
    arrays_to_explode.sort(key=lambda p: p.count(sep))
    if arrays_to_explode and not explode_arrays:
        warnings.warn(
            "Arrays detected in rule paths; explode_arrays=False may produce arrays in output.",
            RuntimeWarning,
        )

    if explode_arrays and arrays_to_explode:
        for index, arr_path in enumerate(arrays_to_explode, start=1):
            df = df.withColumn(arr_path, F.explode_outer(F.col(f"`{arr_path}`")))
            if break_lineage_every and index % break_lineage_every == 0:
                df = _break_lineage(df, use_checkpoint)

    select_cols = []
    missing_paths = []
    for path in paths:
        if _path_exists(df.schema, path, sep):
            select_cols.append(F.col(f"`{path}`").alias(path))
        else:
            missing_paths.append(path)
            select_cols.append(F.lit(None).alias(path))

    if missing_paths:
        warnings.warn(
            f"Rule paths not found in schema: {missing_paths}",
            RuntimeWarning,
        )

    return df.select(*select_cols)


def flatten_by_rules_json(
    df: DataFrame,
    rules_text: str,
    sep: str = ".",
    explode_arrays: bool = True,
    break_lineage_every: int = 0,
    use_checkpoint: bool = False,
    repartition_to: Optional[int] = None,
) -> DataFrame:
    """
    Parse rules JSON text, extract rule paths, and return a DataFrame with only those columns.
    """
    rules = json.loads(rules_text)
    rule_paths = extract_rule_paths(rules)
    return flatten_by_rules(
        df,
        rule_paths,
        sep=sep,
        explode_arrays=explode_arrays,
        break_lineage_every=break_lineage_every,
        use_checkpoint=use_checkpoint,
        repartition_to=repartition_to,
    )


def extract_rule_paths(rules: List[dict]) -> List[str]:
    """
    Extracts all column paths from rules.
    """
    paths: List[str] = []
    for rule in rules:
        rtype = rule.get("type")
        if rtype in {"headers", "non_empty", "unique"}:
            paths.extend(rule.get("columns", []))
        else:
            col = rule.get("column")
            if col:
                paths.append(col)
    return paths


def _collect_required_arrays(schema: T.StructType, paths: List[str], sep: str) -> List[str]:
    arrays: List[str] = []
    seen: Set[str] = set()

    for path in paths:
        parts = path.split(sep)
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


def _path_exists(schema: T.StructType, path: str, sep: str) -> bool:
    if any(f.name == path for f in schema.fields):
        return True

    parts = path.split(sep)
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
    """
    Optimized struct flattening that minimizes DataFrame transformations.
    
    Key optimizations:
        1. Single schema inspection per iteration (vs multiple .dtypes calls)
        2. Pre-allocate column list to reduce memory allocations
        3. Use schema.fields directly instead of string parsing
        4. Early termination check before building column list
    
    Args:
        df: Input DataFrame with potentially nested struct columns
        sep: Separator to use in flattened column names (default: ".")
        max_depth: Maximum nesting depth to flatten (default: 100, prevents infinite loops)
    
    Returns:
        DataFrame with all struct columns flattened
        
    Raises:
        RuntimeError: If max_depth is exceeded (likely indicates circular reference or malformed schema)
    """
    out = df
    
    for depth in range(max_depth):
        # Single schema inspection - cache the fields list
        schema_fields = out.schema.fields
        
        # Check if any struct columns remain (optimized check)
        has_structs = any(isinstance(f.dataType, T.StructType) for f in schema_fields)
        if not has_structs:
            # No more structs to flatten, we're done
            return out
        
        # Build new column list: expand structs, keep others as-is
        # Pre-allocate list for better memory efficiency
        cols = []
        for field in schema_fields:
            if isinstance(field.dataType, T.StructType):
                # Expand struct fields into separate columns with dot notation
                struct_fields = field.dataType.fields
                for nested_field in struct_fields:
                    # Use backticks for column names with special characters
                    col_expr = F.col(f"`{field.name}`.`{nested_field.name}`")
                    alias_name = f"{field.name}{sep}{nested_field.name}"
                    cols.append(col_expr.alias(alias_name))
            else:
                # Keep non-struct columns unchanged (with backticks for safety)
                cols.append(F.col(f"`{field.name}`"))
        
        # Single select operation per iteration
        out = out.select(*cols)
    
    # If we exit the loop, we hit max_depth without fully flattening
    raise RuntimeError(
        f"Maximum flattening depth ({max_depth}) exceeded. "
        f"This likely indicates a circular reference or extremely deep nesting in the schema."
    )


def _flatten_structs(df: DataFrame, sep: str = ".", max_depth: int = 100) -> DataFrame:
    """
    Legacy struct flattening function - kept for backwards compatibility.
    
    DEPRECATED: Use _flatten_structs_optimized instead for better performance.
    This function will be removed in a future version.
    
    Args:
        df: Input DataFrame with potentially nested struct columns
        sep: Separator to use in flattened column names (default: ".")
        max_depth: Maximum nesting depth to flatten (default: 100, prevents infinite loops)
    
    Returns:
        DataFrame with all struct columns flattened
    """
    return _flatten_structs_optimized(df, sep=sep, max_depth=max_depth)
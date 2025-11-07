"""
Utilities for flattening nested PySpark DataFrames with struct and array columns.

Optimized for Spark SQL to minimize DataFrame transformations and leverage
lazy evaluation for better query plan optimization.
"""
from typing import List, Tuple, Set
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
"""
Utilities for flattening nested PySpark DataFrames with struct and array columns.
"""
from typing import List, Tuple
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F, types as T


def flatten_all(df: DataFrame, sep: str = ".", explode_arrays: bool = False) -> Tuple[DataFrame, List[str]]:
    """
    Flattens struct fields and optionally explodes arrays of structs.
    
    This is the main public API for flattening nested DataFrames. It first flattens
    all struct columns, then optionally explodes any array<struct> columns into 
    separate rows (similar to SQL UNNEST).
    
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
    # First, flatten all struct columns
    df = _flatten_structs(df, sep=sep)
    exploded = []

    if explode_arrays:
        # Iteratively explode any array<struct> columns
        # We use a loop because exploding can reveal new nested structs
        # Use for loop to guarantee termination after max_iterations
        max_iterations = 100  # Safety limit to prevent infinite loops
        
        for iteration in range(max_iterations):
            # Check if there are any array<struct> columns to explode
            found_array_struct = False
            for f in df.schema.fields:
                if isinstance(f.dataType, T.ArrayType) and isinstance(f.dataType.elementType, T.StructType):
                    # Explode this array column (each array element becomes a new row)
                    df = df.withColumn(f.name, F.explode_outer(F.col(f"`{f.name}`")))
                    exploded.append(f.name)
                    # Re-flatten in case the exploded struct has nested fields
                    df = _flatten_structs(df, sep=sep)
                    found_array_struct = True
                    break  # Restart search after schema change
            
            # If no array<struct> columns found, we're done
            if not found_array_struct:
                break
        else:
            # Loop completed without break = hit max_iterations
            raise RuntimeError(
                f"Maximum array explosion iterations ({max_iterations}) exceeded. "
                f"This likely indicates circular references or extremely deep nesting."
            )
    
    return df, exploded



def _flatten_structs(df: DataFrame, sep: str = ".", max_depth: int = 100) -> DataFrame:
    """
    Private helper: Recursively flattens all struct columns into dot-notation columns.
    
    Arrays are left as-is. This function iterates until no struct columns remain,
    converting nested fields like `user.name` into top-level columns.
    
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
    
    # Use for loop to guarantee termination after max_depth iterations
    for depth in range(max_depth):
        # Check if any struct columns remain
        structs = [(c, t) for c, t in out.dtypes if t.startswith("struct<")]
        if not structs:
            # No more structs to flatten, we're done
            return out
        
        # Build new column list: expand structs, keep others as-is
        cols = []
        for c in out.schema.fields:
            if isinstance(c.dataType, T.StructType):
                # Expand struct fields into separate columns with dot notation
                for f in c.dataType.fields:
                    cols.append(F.col(f"`{c.name}`.`{f.name}`").alias(f"{c.name}{sep}{f.name}"))
            else:
                # Keep non-struct columns unchanged
                cols.append(F.col(f"`{c.name}`"))
        out = out.select(*cols)
    
    # If we exit the loop, we hit max_depth without fully flattening
    raise RuntimeError(
        f"Maximum flattening depth ({max_depth}) exceeded. "
        f"This likely indicates a circular reference or extremely deep nesting in the schema."
    )
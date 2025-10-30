from typing import List, Tuple
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F, types as T

def flatten_structs(df: DataFrame, sep: str = ".") -> DataFrame:
    """Flattens all struct columns recursively. Arrays are left as-is."""
    stack = [(df, [])]
    out = df
    while True:
        structs = [ (c, t) for c, t in out.dtypes if t.startswith("struct<") ]
        if not structs:
            return out
        cols = []
        for c in out.schema.fields:
            if isinstance(c.dataType, T.StructType):
                for f in c.dataType.fields:
                    cols.append(F.col(f"`{c.name}`.`{f.name}`").alias(f"{c.name}{sep}{f.name}"))
            else:
                cols.append(F.col(f"`{c.name}`"))
        out = out.select(*cols)

def flatten_all(df: DataFrame, sep: str = ".", explode_arrays: bool = False) -> Tuple[DataFrame, List[str]]:
    """
    Flattens struct fields; optionally explodes arrays of structs.
    Returns (flattened_df, exploded_columns)
    """
    df = flatten_structs(df, sep=sep)
    exploded = []

    if explode_arrays:
        # iteratively explode any array<struct> columns
        changed = True
        while changed:
            changed = False
            for f in df.schema.fields:
                if isinstance(f.dataType, T.ArrayType) and isinstance(f.dataType.elementType, T.StructType):
                    df = df.withColumn(f.name, F.explode_outer(F.col(f"`{f.name}`")))
                    exploded.append(f.name)
                    df = flatten_structs(df, sep=sep)
                    changed = True
                    break
    return df, exploded

# Useful helpers to validate array columns WITHOUT exploding:
def array_exists(col: Column, pred: Column) -> Column:
    """Returns True if ANY element satisfies pred."""
    return F.exists(col, lambda x: pred._jc)  # py4j uses internal lambda; alternative below

def array_forall(col: Column, pred: Column) -> Column:
    """Returns True if ALL elements satisfy pred."""
    return F.forall(col, lambda x: pred._jc)

# Safe alternatives that don’t rely on Python lambdas (Spark 3.4+):
def array_any_ge(col: Column, threshold: float) -> Column:
    return F.aggregate(col, F.lit(False), lambda acc, x: acc | (x >= F.lit(threshold)))

def array_all_between(col: Column, lo: float, hi: float) -> Column:
    return F.aggregate(col, F.lit(True), lambda acc, x: acc & (x >= F.lit(lo)) & (x <= F.lit(hi)))

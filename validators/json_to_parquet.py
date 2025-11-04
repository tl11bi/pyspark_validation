"""json_to_parquet.py

Lightweight JSON (array of objects) -> Parquet converter preserving nested structure.

Usage (examples):
	python json_to_parquet.py --input tests/data/sample_json_data.json \
		--output tests/data/sample_json_data_parquet --mode overwrite --single-file

Features:
  * Preserves nested structs and arrays (does NOT flatten)
  * Creates output directory if missing
  * Overwrite or fail if exists
  * Optionally coalesce to a single parquet file ("--single-file")

Requirements:
  * pyarrow (preferred) OR fastparquet (fallback limited)

If pyarrow is not installed you'll get a clear message.
"""

from __future__ import annotations

import argparse
import json
import sys
import shutil
from pathlib import Path
from typing import Any, List, Dict


def load_json(path: Path) -> List[Dict[str, Any]]:
	text = path.read_text(encoding="utf-8")
	try:
		data = json.loads(text)
	except json.JSONDecodeError as e:
		raise SystemExit(f"ERROR: Failed to parse JSON file '{path}': {e}")
	if isinstance(data, list):
		return data
	# If file is newline-delimited JSON objects
	if isinstance(data, dict):
		# allow single object by wrapping
		return [data]
	raise SystemExit("ERROR: Top-level JSON must be an array or object.")


def to_table_pyarrow(rows: List[Dict[str, Any]]):
	try:
		import pyarrow as pa  # type: ignore
	except ImportError:
		return None
	# Let pyarrow infer schema including nested structs/arrays
	return pa.Table.from_pylist(rows)


def write_parquet_pyarrow(table, output_dir: Path, single_file: bool):
	import pyarrow.parquet as pq  # type: ignore
	if single_file:
		file_path = output_dir.with_suffix("")  # ensure no .parquet suffix in dir name
		# If user wants a single file, allow specifying either a directory or file root.
		if output_dir.suffix.lower() == ".parquet":
			file_path = output_dir
		else:
			file_path = output_dir / "data.parquet"
		file_path.parent.mkdir(parents=True, exist_ok=True)
		pq.write_table(table, file_path)
	else:
		output_dir.mkdir(parents=True, exist_ok=True)
		pq.write_table(table, output_dir / "part-0.parquet")


def write_parquet_fastparquet(rows: List[Dict[str, Any]], output_dir: Path, single_file: bool):
	try:
		from fastparquet import write as fp_write  # type: ignore
	except ImportError:
		raise SystemExit("ERROR: Neither pyarrow nor fastparquet is installed. Install pyarrow: pip install pyarrow")
	# fastparquet expects a pandas DataFrame
	try:
		import pandas as pd  # type: ignore
	except ImportError:
		raise SystemExit("ERROR: fastparquet fallback requires pandas. Install pyarrow instead for best results.")
	df = pd.DataFrame(rows)
	output_dir.mkdir(parents=True, exist_ok=True)
	fp_write(output_dir / "part-0.parquet", df)
	if single_file:
		# already single file; nothing else to do
		pass


def parse_args(argv: List[str]) -> argparse.Namespace:
	p = argparse.ArgumentParser(description="Convert nested JSON array file to Parquet (no flattening).")
	p.add_argument("--input", required=True, help="Path to JSON file containing an array (or single object).")
	p.add_argument("--output", required=True, help="Output directory (or file if --single-file with .parquet).")
	p.add_argument("--mode", choices=["overwrite", "error", "ignore"], default="error",
				   help="Behavior if output exists: overwrite (delete and recreate), error, or ignore.")
	p.add_argument("--single-file", action="store_true", help="Write a single Parquet file instead of a directory of parts.")
	return p.parse_args(argv)


def prepare_output(path: Path, mode: str):
	if path.exists():
		if mode == "overwrite":
			if path.is_dir():
				shutil.rmtree(path)
			else:
				path.unlink()
		elif mode == "error":
			raise SystemExit(f"ERROR: Output path '{path}' already exists. Use --mode overwrite to replace.")
		elif mode == "ignore":
			print(f"Output path '{path}' exists; mode=ignore -> skipping conversion.")
			return False
	return True


def main(argv: List[str]) -> int:
	args = parse_args(argv)
	input_path = Path(args.input)
	output_path = Path(args.output)

	if not input_path.exists():
		raise SystemExit(f"ERROR: Input file '{input_path}' does not exist.")

	# If single-file and user gave a directory-like path without .parquet, we still allow it.
	can_proceed = prepare_output(output_path, args.mode)
	if not can_proceed:
		return 0

	rows = load_json(input_path)
	print(f"Loaded {len(rows)} JSON object(s) from {input_path}")

	table = to_table_pyarrow(rows)
	if table is not None:
		print("Using pyarrow backend")
		write_parquet_pyarrow(table, output_path, args.single_file)
	else:
		print("pyarrow not installed; attempting fastparquet fallback")
		write_parquet_fastparquet(rows, output_path, args.single_file)

	print(f"Parquet written to {output_path}")
	return 0


if __name__ == "__main__":  # pragma: no cover
	raise SystemExit(main(sys.argv[1:]))


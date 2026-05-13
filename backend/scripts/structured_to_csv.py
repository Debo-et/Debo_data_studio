#!/usr/bin/env python3
"""
Convert JSON, Avro, or Parquet to CSV.
Usage: structured_to_csv.py <input_file> <format> <output_file>
format: json, avro, parquet

Parquet uses pyarrow (required). JSON and Avro as before.
"""
import sys
import csv
import json
from typing import Any, Dict, List

from logger import setup_logger, log_state_and_exit

# fastavro for Avro
try:
    import fastavro
except ImportError:
    fastavro = None

# pyarrow for Parquet (REQUIRED)
import pyarrow.parquet as pq

log = setup_logger('structured_to_csv')

# ----------------------------------------------------------------------
#  JSON → list of dicts
# ----------------------------------------------------------------------
def parse_json_file(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    records = []
    try:
        data = json.loads(content)
        if isinstance(data, list):
            records = data
        else:
            records = [data]
    except json.JSONDecodeError:
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, list):
                    records.extend(obj)
                elif isinstance(obj, dict):
                    records.append(obj)
            except json.JSONDecodeError:
                log.warning(f"Skipping invalid JSON line: {line[:100]}")
    return records

# ----------------------------------------------------------------------
#  Avro → list of dicts
# ----------------------------------------------------------------------
def parse_avro_file(file_path: str) -> List[Dict[str, Any]]:
    if fastavro is None:
        raise ImportError("fastavro is required for Avro conversion. Install with: pip install fastavro")
    records = []
    with open(file_path, 'rb') as fh:
        reader = fastavro.reader(fh)
        for record in reader:
            records.append(record)
    log.info(f"Avro file parsed, {len(records)} records")
    return records

# ----------------------------------------------------------------------
#  Parquet → list of dicts   (pyarrow ONLY)
# ----------------------------------------------------------------------
def parse_parquet_file(file_path: str) -> List[Dict[str, Any]]:
    """Read Parquet file using pyarrow. This is the only method now."""
    log.info("Using pyarrow for Parquet")
    table = pq.read_table(file_path)
    df = table.to_pandas()
    records = df.to_dict(orient='records')
    log.info(f"Parquet file parsed with pyarrow, {len(records)} records")
    return records

# ----------------------------------------------------------------------
#  CSV writer
# ----------------------------------------------------------------------
def write_csv(records: List[Dict[str, Any]], output_file: str) -> None:
    if not records:
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            pass
        log.warning("No records found; empty CSV written.")
        return

    all_keys = set()
    for rec in records:
        all_keys.update(rec.keys())
    headers = sorted(all_keys)
    log.debug(f"CSV headers: {headers}")

    with open(output_file, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.DictWriter(outf, fieldnames=headers, restval='', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)
    log.info(f"Wrote {len(records)} rows to {output_file}")

# ----------------------------------------------------------------------
#  Main
# ----------------------------------------------------------------------
def main():
    log.debug(f"Arguments: {sys.argv}")
    if len(sys.argv) != 4:
        print("Usage: structured_to_csv.py <input_file> <format> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    fmt = sys.argv[2].lower()
    output_file = sys.argv[3]

    records = []
    try:
        if fmt == 'json':
            records = parse_json_file(input_file)
        elif fmt == 'avro':
            records = parse_avro_file(input_file)
        elif fmt == 'parquet':
            records = parse_parquet_file(input_file)
        else:
            log_state_and_exit(log, f"Unsupported format: {fmt}")

    except Exception as e:
        log_state_and_exit(log, f"Failed to read/parse input file: {e}")

    write_csv(records, output_file)


if __name__ == '__main__':
    main()